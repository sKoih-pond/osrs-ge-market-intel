# Kickoff decisions (section 9 of the discovery report)

**Date:** 20 October 2026
**Decided by:** Sylvester, in a walkthrough of `docs/research/2026-10-20_discovery_report.md` section 9.
**Status:** DECISION. Each item becomes an ADR in M0. Where this log differs from the discovery report (sections 6.3, 6.6 and 7), this log wins.

## Decisions

| # | Topic | Decision | Consequences |
|---|---|---|---|
| 1 | Runtime | **Option B now, Option C committed after v1.** GitHub Actions provides the hourly clock and compute until v1; then the platform moves to an always-on Oracle Cloud Always Free VM (Docker Compose). | Phase C is a planned milestone, not an optional extra. Storage stays in R2 across both phases so the move does not depend on the VM's disk. Re-check the Oracle allowance before Phase C (reports of a 2026 reduction are unverified). |
| 2 | Storage | **Cloudflare R2** on the existing Cloudflare account. | Sylvester enables R2 (Cloudflare may ask for a payment method) and sets a usage notification; there is no hard spending cap. Target usage about a tenth of the free limits. |
| 3 | Orchestrator | **Airflow 3, adopted at Phase C.** No Dagster. | Phase B code is written as Airflow-ready tasks: each step is a plain Python command that takes a data interval (start, end), is safe to re-run, and records completion in the storage layer. GitHub Actions calls these commands in Phase B; Airflow DAGs call the same commands in Phase C, with dbt run through Astronomer Cosmos. The migration is proven by a parity check: partitions built under Airflow must hash-match those built under Actions. Trade-off accepted: no orchestrator UI until after v1. |
| 4 | Backfill | **Full hourly history from 8 Mar 2021, plus 5-minute data from 30 Sep 2026 and the 90 days before; official history from Weird Gloop.** | About 49k + 32k + 4k requests at no more than 1 per second with gzip. Sylvester posts in the Wiki's #api-discussion Discord channel before the backfill starts. |
| 5 | User-Agent contact | **Repo URL plus sylvester@kohstack.au.** | Held in configuration; the client refuses to run without a descriptive User-Agent. |
| 6 | Persona | **Casual is the default, centred in a three-way switch: Flipper on the left, Casual in the middle, Holder on the right.** | The execution policy becomes a first-class, versioned dimension (`policy_id` on every label, backtest, score and paper-record row). Draft parameters (finalised in the M0 policy ADR): Flipper 1 h entry window and 6 h exit horizon; Casual 4 h and 24 h; Holder 24 h and 7 days. Default rankings before ML: net margin after tax for Flipper and Casual, mean reversion for Holder. Build order: Casual, then Flipper, then Holder. Capital (default 50M, range 10M to 500M) and slot count are adjustable in the browser. |
| 7 | ML gate | **Pre-registered gate, per persona.** | A model replaces that persona's baseline only if, on post-switch walk-forward tests, the lower end of the 90% day-block bootstrap interval for its gain in net GP per slot-day is above zero and its Brier score is no worse. Rule and test weeks are written in an ADR before any post-switch results are viewed. A failed gate means the baseline stays and the negative result is published. |
| 8 | Spark and table format | **PySpark for (1) the backtest and sensitivity grid and (2) full-history reprocessing with a parity check against the DuckDB path. Delta Lake as the table format from the start. No Spark for the one-off raw-to-typed conversion.** | Spark never runs in the hourly path. In Phase B, Spark jobs run as hand-started GitHub Actions workflows (16 GB, 4 cores) and on small samples locally; in Phase C, Airflow schedules them. Hourly writes to Delta use Python (`deltalake`, delta-rs). One writer per table at a time, enforced by the orchestrator (Actions concurrency groups, then Airflow pools). A DuckDB vs Polars vs PySpark benchmark is written up alongside the reprocessing job. |
| 9 | Dashboard | **A Cloudflare Worker with static assets, routed at `kohstack.tech/ge/*`.** | Static asset requests are free and unlimited; only script invocations count toward the free plan's 100k requests a day, and any such route is set to fail open. M1 must: exclude `/ge/` from the zone's 30-day edge-cache rule, ship the GE project's own security headers (`_headers`), build the app under the `/ge/` base path, and cover `www`. Sylvester creates a Cloudflare API token scoped to Workers scripts and routes on the kohstack.tech zone (the existing token only purges the cache). The same-tab morph from the portfolio tile is added in M6 through the usual website workflow. |
| 10 | Publication | **Dashboard outputs plus a small real-data sample (for example 30 days x 100 items) in the repo; raw archive private.** | R2 is split into a private area (bronze, silver, gold, manifests) and a public area (`public/v1`). Published data carries attribution to the OSRS Wiki, Jagex and Weird Gloop and the CC BY-NC-SA 3.0 notice. A full dataset release stays open and would need the Wiki team's agreement. |
| 11 | Repository | **Public from day one.** | Unlimited Actions minutes on standard runners; secret scanning and push protection on from the first commit. A private repo would exceed the free plan's 2,000 minutes a month once hourly runs start. Scheduled workflows switch off after 60 days without repository activity, so the watchdog also checks that the schedule itself is alive. |
| 12 | Real-trade check | **Yes, during M4.** | Sylvester places and logs 30 to 50 manual trades under the Casual policy once the simulator exists. Predicted vs actual fill rate, time to fill and price are compared; only a summary is published. This is a sanity check, not training data. |

## Revised milestone outline

This replaces the milestone sequence in the discovery report, section 7. Acceptance criteria there still apply unless changed here.

| Milestone | Change from the report |
|---|---|
| M0 Kickoff | Adds: one-day spike proving `deltalake` (delta-rs) and Spark (delta-spark over S3A) both write safely to R2, and confirming how dbt-duckdb outputs land in Delta (DuckDB reads Delta well; the write path needs checking); Worker static-assets test on a path prefix, including the cache-rule interplay; policy ADR for the three personas; ML gate pre-registration template; Discord post. |
| M1 Live screener | Casual persona only; GitHub Actions calls Airflow-ready task commands; Delta tables in R2; Worker deployment at `/ge/`. |
| M2 Historical foundation | Unchanged scope; conversion of raw history uses DuckDB, not Spark. |
| M3 Hardening | Dagster items replaced by: task interface contract (interval in, idempotent, completion recorded), Delta log as the completeness record, data checks as dbt tests and Python check tasks, container image in GHCR. |
| M4 Intelligence I | Backtest and sensitivity grid built as a PySpark job (hand-started Actions runs); Casual first, then Flipper and Holder; pre-registered gate per persona; real-trade logging. |
| M5 Intelligence II | Unchanged. |
| M6 Product | Adds the Flipper / Casual / Holder switch and the portfolio tile with the same-tab morph. |
| **v1 release** | M1 to M3, the M5 data-quality gate and simple anomalies, the Casual screener with Data and Execution grades, the live paper record. |
| **M7 Platform move (Phase C)** | New. Oracle VM with Docker Compose, Airflow 3 and Postgres, dbt through Cosmos, Spark reprocessing job with DuckDB parity check, migration parity by partition hashes, TLS, backups, failure alerts, runbook. |
| M8 Production and portfolio | The report's M7 content: drift monitoring, promotion guard with shadow week, rollback, case study, Spark benchmark write-up, README and demo. |

## Actions owned by Sylvester (no agent does these)

- Post in the OSRS Wiki Discord #api-discussion channel before the M2 backfill.
- Enable R2 and set a usage notification (M0 or M1).
- Create the scoped Cloudflare API tokens: Workers scripts and routes on kohstack.tech, and an R2 token limited to the project bucket.
- Approve creation of the public GitHub repository.
- Phase C: create the Oracle Cloud account.
- M4: place and log the manual trades.

## Open items to verify, not assume

- Safe concurrent-write behaviour of delta-rs and delta-spark on R2 (M0 spike).
- Workers static assets under a path prefix alongside the zone's existing cache rules (M0 test).
- The current Oracle Always Free allowance and idle-reclamation thresholds (before M7).
- GitHub Actions terms for scheduled data jobs (watch for changes; fallback is moving Phase C earlier).

## Amendments

- **2026-10-20, decision 4 (backfill source).** Historical data comes from the Kaggle dump `antonygarand/osrs-ge-5m` v1, validated month by month against the Wiki API, with gaps filled from the API (ADR-0005 note; assessment in `docs/research/`). The Discord post is now a courtesy plus two questions rather than a prerequisite for heavy load.
- **2026-10-20, Wiki Discord post done.** Sylvester posted in #api-discussion; the community pointed him to the Kaggle dump that became the history source. The two open questions (a bulk Weird Gloop route; whether the high/low definition changed with the Jagex feed) stay open unless answered there.
- **2026-10-20, R2 key scope.** Sylvester keeps one Object Read & Write key across all R2 buckets while the account holds only this project's buckets, and will rotate keys when another project adds buckets. ADR-0015 assumed bucket-limited keys; this is an accepted, recorded deviation.

- **2026-10-20, ADR-0015 (Sylvester: R2 must stay inside the free limit, prevention not alerts).** Nothing in R2 is ever public: the public bucket from decision 10 is dropped and published data ships as Workers static assets (decision 9). Standard storage class only; `bronze/` expires after 30 days; backfill raw responses stay on local disk; a usage check before every run halts at 50% of any free allowance or 6 GB stored; per-run operation caps; Spark and backfills work on local copies.
