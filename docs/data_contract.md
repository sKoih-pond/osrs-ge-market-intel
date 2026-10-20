# Data contract v1

The dashboard reads two files, published hourly as Workers static assets beside the page (ADR-0012, ADR-0015). Both are validated against their JSON Schema (draft 2020-12) before every deploy, and the schemas ship with the data.

| File | URL | Schema |
|---|---|---|
| Screener snapshot | `https://kohstack.tech/ge/data/v1/snapshot.json` | `contracts/v1/snapshot.schema.json` (published at `/ge/data/v1/schema/`) |
| Pipeline status | `https://kohstack.tech/ge/data/v1/status.json` | `contracts/v1/status.schema.json` |

Conventions: prices and amounts are whole GP (integers); times are UTC, ISO 8601 with a `Z`; `contract_version` follows semantic versioning, and a `1.x` change only adds optional fields. Removing or renaming a field means `v2/` alongside `v1/`.

## `snapshot.json`

One closed hour (`data_as_of` is its end, exclusive) under one execution policy (`policy`, casual-v1 in M1).

- `ideas`: at most 100, ranked by `net_gp` (net margin after tax times feasible quantity, ADR-0008). Only grade A or B data, positive net margin and a feasible quantity of at least 1. Each idea carries the inputs behind its numbers: the price window, last trade times, volumes, the Data grade and its one-line reason.
- `counts`: how many items were priced, how many cleared the tax with sound data, and `withheld_grade_c_with_margin`, the items that showed a margin but whose data was grade C.
- `tax`, `data_grade`: the rule and thresholds in force, so a reader can check any number.
- `attribution`, `licence`: the data is CC BY-NC-SA 3.0 (ADR-0013).

## `status.json`

- `data_as_of` and `stale_after_minutes` (120): a page shows its staleness warning when now minus `data_as_of` exceeds the limit (AT3).
- `freshness_30d`: hours due in the last 30 days, and how many were published (deployed) within 45 minutes of closing (AT4: target 95%). Counting starts at the first deployed hour.
- `completeness_30d`: 5-minute buckets expected vs recorded, and how many were source outages (`docs/methodology/outage_rule.md`).
- `days`: the same figures per UTC day.
- `last_successful_run`: the newest `hourly` run that finished successfully.

## Field definitions

The page's "Definitions" section explains each number in plain words; `dbt/models/marts/market_snapshot.yml` and the schemas define them precisely.
