# Outage or quiet market? (5-minute buckets)

**Status:** in use from M1. **Code:** `ge_intel.ingest.classify`, `OUTAGE_RATIO = 0.6`. **Rule source:** ADR-0005 ("an item missing from a bucket means no trades; a bucket whose item count falls far below its trailing median is flagged as a source outage").

## The rule

- An **item missing** from a 5-minute bucket means that item did not trade in those 5 minutes. No row is written for it; nothing is imputed.
- A **bucket** is recorded as an **outage** when it holds fewer than **0.6 times** the median item count of the `ok` buckets in the 24 hours before it (needs at least 12 such buckets to judge). Its rows are still stored, and the manifest marks the bucket so the mart never reads its thin coverage as a quiet market: a price hour containing an outage bucket gets Data grade C.
- A bucket that comes back **empty within 15 minutes of closing** is treated as not yet published, not as an outage: nothing is recorded and the next run asks again. Empty and older than that, it is recorded as an outage.

## Why 0.6

| Observation | Items in the bucket | Ratio to the 24-hour median (1,848) |
|---|---:|---:|
| Quietest normal 5-minute bucket, 18 to 19 Oct 2026 (08:10 UTC) | 1,462 | 0.79 |
| Weekly game update, 14 Oct 2026, 10:30 UTC bucket | 751 | 0.41 |
| Weekly game update, 7 Oct 2026, 11:00 UTC hour (`/1h`, vs its own median of 3,291) | 1,548 | 0.47 |

The threshold sits midway between the quietest normal bucket and the update buckets, with room on both sides. Quiet early-morning UTC hours (the trough of the daily cycle) stay well above it. Reproduce with `python scripts/derive_data_grade.py` (last line) and the raw responses from `scripts/fetch_raw.py`.

## Limits

- One day of 5-minute data and two update events. Re-check once the M2 backfill gives months of buckets, and add the update calendar (ADR-0006) as a second signal.
- An outage that thins coverage only slightly (above 0.6) is not caught by this rule. The Data grade's coverage inputs still see it.
