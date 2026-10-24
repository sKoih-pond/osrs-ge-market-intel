# ADR-0016: M1 live screener: implementation decisions

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Agent, within the M1 scope set by Sylvester; measured where noted. Amends ADR-0004, 0005, 0009, 0012, 0014 and 0015 (each carries a note).

## Context

M1 turns the M0 decisions into a running pipeline: hourly ingestion of `/5m`, `/latest` and `/mapping` into Delta tables on R2, one dbt-duckdb mart, a published v1 data contract, an assets-only Worker at `kohstack.tech/ge/`, a watchdog and an hourly GitHub Actions workflow. Building it forced choices the earlier ADRs left open. This record collects them so each can be challenged on its own.

## Decisions

### Tasks and scheduling (ADR-0004)

1. **Registry and entry points.** `ge_intel.tasks.REGISTRY` lists every task with its upstream tasks, cadence (`gap-fill`, `hourly`, `daily`, `weekly`) and the tables it writes. `ge-intel run <task> --start --end` is the Airflow-ready interface; `ge-intel hourly` is the Phase B scheduler shim that walks the registry in order for the newest closed hour. `ge-intel tasks` prints the registry as JSON for DAG generation in M7.
2. **Completion.** A task skips an interval already recorded as `success` in `ops/task_runs` unless `--force`. `ingest_5m` is the exception: the bucket manifest (`silver/wiki_5m_buckets`) is its completeness record, so it always asks storage what is missing. Each process writes its `ops/task_runs` rows in **one commit at exit**, and writes nothing when every task was already complete.
3. **Two schedule slots an hour** (`:06` and `:26`; first `:07` and `:27`, changed the same day because GitHub never fired the original schedule, see the runbook). GitHub drops or delays scheduled runs under load; with one slot, every dropped run is a late hour against AT4. The second slot is a no-op when the first succeeded (no R2 writes, a few reads).
4. **dbt runs in its own process.** dbt-duckdb keeps its DuckDB connection open in-process, which blocks reading the mart afterwards; a subprocess (as Cosmos will run it in Phase C) isolates it.

### Ingestion (ADR-0005)

5. **Gap-fill newest first, 96 buckets per run** (amended by ADR-0018: the scheduler looks back 7 days at most). The newest buckets come first so the current hour publishes even mid-catch-up; older gaps follow on later runs. 96 buckets is about 100 seconds of requests at 1 per second and about 100 `PutObject` calls, inside the per-run caps. A one-hour skip (12 buckets) is always filled by the next run.
6. **Write order for crash safety.** Facts (`silver/prices_5m`, replace-where on exactly the fetched buckets) commit before the manifest. A crash between them leaves the bucket unrecorded, so the next run re-fetches it and the replace-where overwrites instead of duplicating. Tested.
7. **Outage rule: 0.6 of the trailing 24-hour median item count**, with 15 minutes' grace for an empty bucket that may not be published yet. Derivation: `docs/methodology/outage_rule.md`.
8. **Bronze keys include a content hash:** `bronze/wiki/<endpoint>/date=<day>/<bucket epoch>-<sha256[:12]>.json.gz`. Keys stay deterministic from (endpoint, bucket, content); a revised response becomes a second object instead of being silently lost, and writes are write-if-absent.
9. **Live-only sources.** `/latest` and `/mapping` take no timestamp, so their tasks refuse intervals more than an hour (or a day) old and record a skip, rather than filing today's state under an old date. Staleness for the Data grade is measured from `/latest` at fetch time, so it includes the run's own delay.
10. **Measurement regime boundary at 00:00 UTC on 30 September 2026**, the same day-start approximation as the tax eras. The M2 break detector will pin the exact instant.

### Grading and the mart (ADR-0008, ADR-0009)

11. **Data grade (v2)** thresholds come from measured next-hour price drift and two-sided trading: A = both sides traded in all 24 of the last 24 hours, the staler side within 10 minutes, and the price hour within 5% of the item's own 24-hour median; C = fewer than 16 two-sided hours, staler side over 30 minutes, the price hour more than 10% from its median, an incomplete or outage price hour, or an incomplete 24-hour window. **Volume was tested and is not used** (it did not separate stable from unstable prices). v1 lacked the median check and in its first live hour ranked a single stray trade (Yellow boots, sell price 85 times the buy price) first; v2 replaced it the same day. Derivation: `docs/methodology/data_grade.md`.
12. **Prices round against the trader:** buy at the trailing instant-sell VWAP rounded up, sell at the trailing instant-buy VWAP rounded down, both to whole coins before tax (ADR-0007).
13. **"Recent instant-sell volume" in the feasible quantity is the policy's entry window** (4 hours for casual-v1), the same window the buy offer competes in.
14. **One tax implementation.** The mart calls `ge_tax_per_item`, a DuckDB macro generated from `ge_intel.tax`'s eras and cap by a dbt-duckdb plugin; a test checks the macro against the Python function on every edge case. (A Python UDF would have needed numpy.)
15. **The paper record starts.** Every hour's full mart, including `is_idea` and `idea_rank`, is kept in `gold/market_snapshot` under `policy_id = casual-v1`; scoring each idea 24 hours later arrives with the M4 simulator. (ADR-0019, accepted 24 October 2026: every idea row is kept for ever and the full mart for 30 days.)

15a. **Gold tables may gain columns.** The snapshot write uses delta-rs `schema_mode="merge"`, which adds nullable columns without changing the protocol (reader 1, writer 2, no features; tested). Removing or retyping a column still needs a new table or a rebuild.

### Publication and deployment (ADR-0012, ADR-0013, ADR-0015)

16. **v1 data contract:** `snapshot.json` (top 100 ideas, grade A or B only, every number with its UTC data window) and `status.json` (last run, 30-day freshness and completeness, per-day table), each validated against `contracts/v1/*.schema.json` before it ships; the schemas ship beside the data. A violation fails the run and nothing deploys.
17. **A publish counts only once deployed.** `publish` records `success` only after `wrangler deploy` returns; a build without a deploy records `built`. AT4's freshness figure counts first successful publishes only, so it measures what visitors could see.
18. **Deploy gate:** the repository variable `GE_DEPLOY_ENABLED` must be `true` for the hourly workflow to deploy. Until Sylvester sets it, the pipeline ingests, builds and validates every hour without touching `kohstack.tech`.
19. **No build step for the M1 page.** Plain HTML, CSS and JavaScript under `site/src/`, copied with the data into `site/dist/`. Vite (ADR-0012) arrives with the M6 product work. HTML, CSS and JS keep the Workers assets default (`max-age=0, must-revalidate`), data gets 60 seconds.
20. **Staleness is computed in the browser** from `status.json`'s `data_as_of`, so a paused pipeline shows the warning on every page after 2 hours without any redeploy (AT3).

### Cost controls (ADR-0015)

21. **Own SigV4 client for bronze writes** (standard library, checked against AWS's published signature examples) so every `PutObject` passes through `RunBudget.charge` before it is sent; no `HeadObject` is needed because writes are conditional.
22. **The previous run's window is checked in the same GraphQL query** as month-to-date usage (one query per run). A previous run over 500 Class A or 5,000 Class B operations on the bucket blocks the next run until someone reviews it and sets the repository variable `GE_GUARD_ACK_RUN_ID` to that run's id.

### Supply chain (ADR-0014)

23. **Lockfiles:** `uv.lock` for Python, `site/package-lock.json` for Wrangler (pinned to 4.150.0, installed with `npm ci --ignore-scripts`). Every action is pinned to a commit SHA, with secrets passed only to the step that needs them.

## Consequences

- An hour is late against AT4 only if both slots in that hour fail or are dropped, or the run itself fails.
- Steady-state cost per run is a small fraction of the per-run caps. Measured on 19 October 2026 (Cloudflare analytics, settled): bootstrapping 24 hours (288 buckets in three gap-fill runs) plus every other task, as seven separate processes, used 501 Class A and 854 Class B operations in total and left 12.6 MB in the bucket. The heaviest single process, a 96-bucket gap-fill, stays well under its caps of 500 and 5,000.
- The page cannot show an idea whose data is grade C, and says how many were withheld.
- The watchdog depends only on the public `status.json` and the GitHub API.

## Open

- **Preview links:** the account now has a `workers.dev` subdomain (`kohstack`, created for ADR-0017), but preview URLs stay off; turning them on is still Sylvester's call.
- **`www` for `/ge/`** is served, not redirected, matching the portfolio.
- **GitHub's scheduler never fired here**; the clock moved to a Cloudflare Cron Trigger (ADR-0017). The 60-day inactivity rule then matters only for the GitHub schedules kept as a second clock.

## Alternatives considered

- **boto3 for bronze writes:** standard and well tested, but large, and its requests are invisible to `RunBudget` without event hooks.
- **Oldest-first gap-fill:** simpler, but after a long outage the page would stay stale until the whole backlog cleared.
- **A volume threshold in grade A:** rejected on the data (decision 11).
- **One schedule slot:** half the no-op runs, but every dropped run becomes a late hour.

## References

- `docs/methodology/data_grade.md`, `docs/methodology/outage_rule.md`, `docs/data_contract.md`, `docs/runbook.md`.
