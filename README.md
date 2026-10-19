# OSRS Grand Exchange Market Intelligence

A zero-cost data platform that turns 5-minute Grand Exchange trade data into tax-aware trade ideas and anomaly alerts, and shows how far each one can be trusted.

**Status: M0 (kickoff).** Nothing is deployed and nothing runs on a schedule yet. The repository will be public at `github.com/sKoih-pond/osrs-ge-market-intel` once created.

## Why this is an interesting data problem

- The Grand Exchange shows no order book. Only volume-weighted trade prices and volumes per 5-minute bucket are public, so whether an offer would fill has to be modelled.
- A 2% sell-side tax (capped at 5M GP per item) means most small price moves cannot be traded at a profit.
- On 30 September 2026 the public price feed switched source, from RuneLite users to Jagex data across all game clients. Observed volumes rose 1.25x to 1.8x overnight and spreads widened. The pipeline has to detect and handle a structural break in its own source.

## Planned architecture

GitHub Actions runs idempotent, Airflow-ready tasks every hour, writing Delta tables to Cloudflare R2. dbt and DuckDB build the models; PySpark runs the backtest grid and full-history reprocessing. A static dashboard, served by a Cloudflare Worker at kohstack.tech/ge/, reads a versioned data contract. After v1, the platform moves to an always-on VM running Airflow 3. Details in `docs/adr/`.

## Repository map

| Path | Contents |
|---|---|
| `docs/research/` | Discovery report and the probe scripts behind every measured figure |
| `docs/decisions/` | Kickoff decision log (overrides older documents where they differ) |
| `docs/kickoff/` | Problem Brief, Delegation Plan, v1 Acceptance Tests |
| `docs/adr/` | Architecture decision records |
| `docs/spikes/` | Plans (and later results) for the Delta-on-R2 and Worker spikes |
| `src/ge_intel/` | Pipeline package (so far: the tax model, ADR-0007) |
| `tests/` | pytest suite |

## Development

Requires Python 3.12.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/pytest
```

## Data and attribution

Price data: OSRS Wiki real-time prices API (RuneLite data before 30 September 2026, Jagex data after) and Weird Gloop exchange history. OSRS Wiki content is licensed CC BY-NC-SA 3.0. This project is non-commercial, shows no real-money values, and does not automate the game client in any way.

## Licence

Code and documentation: MIT, see [LICENSE](LICENSE) (ADR-0014). The MIT licence does not cover the sample price data, which will be CC BY-NC-SA 3.0 in its own folder with its own notice (ADR-0013).
