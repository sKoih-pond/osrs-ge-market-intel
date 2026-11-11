# OSRS Grand Exchange Market Intelligence

A zero-cost data platform that turns 5-minute Grand Exchange trade data into tax-aware trade ideas and anomaly alerts, and shows how far each one can be trusted.

**Status (1 November 2026): M1 live, M2 in progress.** Since 20 October 2026 an hourly GitHub Actions pipeline, started by a Cloudflare cron trigger, has ingested the OSRS Wiki's 5-minute data into Delta tables on Cloudflare R2, built a tax-aware market snapshot with dbt and DuckDB, and published a validated data contract to [kohstack.tech/ge/](https://kohstack.tech/ge/). In M1's seven-day window, 21 to 27 October, all 168 hours were published within 45 minutes of the hour closing (target 95%); [the status page](https://kohstack.tech/ge/status/) shows the current figures. M2 has loaded the history: 68 months from the Kaggle dump, each spot-checked against the Wiki's `/1h` data, are in R2 as about 138 million hourly rows. The rest of M2, including a second source, the 30 September break profile, reconciliation and the completeness report, is planned (`docs/plans/M2.md`).

## At a glance

| | |
|---|---|
| Live | The hourly screener at [kohstack.tech/ge/](https://kohstack.tech/ge/) since 20 October 2026; 168 of 168 hours on time in M1's first week |
| History | 68 months (March 2021 to October 2026) in R2: 138,268,178 hourly rows |
| Reprocessing | An independent PySpark rebuild of all of it, compared with the DuckDB build and R2: every row, volume and label equal; 8 prices one 0.01 GP rounding step apart |
| Orchestration | Hourly: GitHub Actions started by a Cloudflare cron. History: an Airflow 3 DAG over the same task contract, one mapped group per month (209 tasks, 58 minutes; a second run finds every month done) |
| Benchmark | The same 878M-row roll-up on one 4-core runner: Polars 94 s (peaked at 12 GB), DuckDB 166 s (6 GB), PySpark 296 s (7 GB) |
| Cost | $0: free tiers, and a cost guard that halts at half of R2's free allowance |

Evidence for the last four rows, with run IDs, timings and screenshots: [`docs/evidence/airflow_pyspark.md`](docs/evidence/airflow_pyspark.md).

## Why this is an interesting data problem

- The Grand Exchange shows no order book. Only volume-weighted trade prices and volumes per 5-minute bucket are public, so whether an offer would fill has to be modelled.
- A 2% sell-side tax (capped at 5M GP per item) means most small price moves cannot be traded at a profit.
- On 30 September 2026 the public price feed switched source, from RuneLite users to Jagex data across all game clients. Observed volumes rose 1.25x to 1.8x overnight and spreads widened. The pipeline has to detect and handle a structural break in its own source.

## Architecture

GitHub Actions runs idempotent, Airflow-ready tasks every hour (`ge-intel run <task> --start --end`), writing Delta tables to a private Cloudflare R2 bucket with delta-rs. dbt and DuckDB build the market snapshot; the published data ships as static assets of a Cloudflare Worker at kohstack.tech/ge/, so visitor traffic never touches R2. A cost guard checks R2 usage before every run and fails closed; an independent watchdog opens an issue if data goes stale. PySpark already rebuilds the history independently for a parity check, and an Airflow 3 DAG runs that rebuild on request; the backtest grid follows in M4, and after v1 the hourly path moves to Airflow on an always-on VM. Details in `docs/adr/`, the M1 choices in ADR-0016.

```
Wiki API v2 ──> ingest (/5m gap-fill, /latest, /mapping) ──> R2: bronze (raw, 30 days) + silver Delta tables
                                                               │
                         dbt-duckdb market_snapshot (tax, feasible quantity, Data grade) ──> gold Delta table
                                                               │
                  publish: snapshot.json + status.json, JSON Schema-validated ──> Worker static assets at /ge/
```

## Repository map

| Path | Contents |
|---|---|
| `docs/research/` | Discovery report and the probe scripts behind every measured figure |
| `docs/decisions/` | Kickoff decision log (overrides older documents where they differ) |
| `docs/kickoff/` | Problem Brief, Delegation Plan, v1 Acceptance Tests |
| `docs/adr/` | Architecture decision records |
| `docs/spikes/` | Plans and results of the Delta-on-R2 and Worker spikes |
| `docs/methodology/` | How the Data grade and the outage rule were derived from data |
| `docs/plans/` | Milestone plans (M2: historical foundation) |
| `docs/runbook.md`, `docs/data_contract.md` | Operating the pipeline; the published v1 contract |
| `src/ge_intel/` | Pipeline package: Wiki client, ingestion, lake, tasks and CLI, tax, cost guard, publish, watchdog |
| `src/ge_intel/seeds/` | Regimes and the era-aware tax-exemption list, read by Python and dbt |
| `dbt/` | dbt-duckdb project: staging views over the Delta tables and the `market_snapshot` mart |
| `contracts/v1/` | JSON Schemas for the published files |
| `site/` | The page (`src/`) and the assets-only Worker config |
| `clock/` | `ge-clock`, the Cloudflare Worker whose Cron Triggers start the hourly run (ADR-0017) |
| `scripts/` | Fetching raw responses, building fixtures, the exemption seed and the grade derivation |
| `engines/` | The batch engines, a project of their own: the PySpark rebuild of `history/prices_1h` and the DuckDB, Polars and PySpark benchmark |
| `orchestration/` | The Airflow 3 history DAG and the harness that runs it on a GitHub runner |
| `docs/evidence/` | What ran, with run IDs, results and screenshots |
| `tests/` | Unit, contract (recorded payloads) and end-to-end tests |

## Development

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/) (the lockfile is `uv.lock`).

```bash
uv sync --locked --extra dev
uv run ruff check . && uv run ruff format --check .
uv run pytest
```

Run the whole pipeline against a local lake (real Wiki data, no credentials; writes `data/lake` and `site/dist`):

```bash
GE_LAKE_ROOT=data/lake uv run ge-intel hourly --skip-guard
```

Operating the scheduled pipeline: `docs/runbook.md`.

## Data and attribution

Price data: OSRS Wiki real-time prices API (RuneLite data before 30 September 2026, Jagex data after). History before live collection began on 18 October 2026 comes from the Kaggle dataset [`antonygarand/osrs-ge-5m`](https://www.kaggle.com/datasets/antonygarand/osrs-ge-5m) (version 1, by Antony Garand), collected from the same API. Weird Gloop's exchange history is planned (M2). OSRS Wiki content is licensed CC BY-NC-SA 3.0, and the dataset, derived from it, is used on the same terms. This project is non-commercial, shows no real-money values, and does not automate the game client in any way.

## Licence

Code and documentation: MIT, see [LICENSE](LICENSE) (ADR-0014). The MIT licence does not cover price data: the recorded responses in `tests/fixtures/wiki/`, the dump-format sample in `tests/fixtures/kaggle/` and the published data files are CC BY-NC-SA 3.0, with their own notice (ADR-0013).
