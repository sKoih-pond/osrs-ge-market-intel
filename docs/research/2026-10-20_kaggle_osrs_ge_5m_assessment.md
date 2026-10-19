# Assessment: Kaggle "OSRS Grand Exchange 5m price series"

**Date:** 20 October 2026
**Dataset:** https://www.kaggle.com/datasets/antonygarand/osrs-ge-5m (Antony Garand, version 1, created 17 October 2026)
**Question:** can this dump replace the Wiki API backfill planned for M2 (ADR-0005)?
**Answer:** yes, as the primary historical source, provided every month is checked against the API and gaps are filled from the API. The decision is Sylvester's (see the end).

## What it is

| Property | Value |
|---|---|
| Source | OSRS Wiki real-time prices API, `/5m` buckets, field names and values unchanged ("No derived columns") |
| Licence | CC BY-NC-SA 3.0, the Wiki's own terms. Compatible with ADR-0013 (non-commercial, attribution, share-alike) |
| Size | 4.80 GB: 68 monthly Parquet files (`prices_5m/year=YYYY/month=MM/data.parquet`, 45 to 79 MB each), `items.parquet`, README |
| Coverage | 8 March 2021 07:35 UTC (the start of Wiki data) to 17 October 2026 13:10 UTC |
| Rows | 876,754,705 item-buckets, 4,539 items (README); rows where neither side traded are omitted |
| Layout | Sorted by item then time, zstd-compressed, partitioned by month |
| Popularity | 1 download, 37 views at the time of checking: new and unreviewed |

Download: unauthenticated through Kaggle's API, one file per request; nested paths must be URL-encoded.

## Validation (two files: March 2021 and October 2026, one per source regime)

Compared with values recorded directly from the Wiki API during discovery (`probes/probe2.py`). The hourly endpoint is an exact sum of twelve 5-minute buckets, so the dump's buckets for 12:00 to 12:55 UTC must reproduce the API's hourly figures.

| Check | Result |
|---|---|
| Hourly item count and total volume, 10 hours (3 in March 2021, 7 in October 2026) | **10 / 10 exact** |
| Per-item high and low volume, 12 item-hours (whip, nature rune, shark, twisted bow, bond, cannonball on 6 and 13 October 2026) | **12 / 12 exact** |
| Rows with both sides empty | 0 |

## Defects and quirks

1. **`timestamp` is a bucket number, not epoch seconds.** The README says epoch seconds aligned to 300; the values are epoch seconds divided by 300 (for example 5,383,963 is 8 March 2021 07:35 UTC). The README's own range ("1970-03-04 .. 1970-03-10") shows the same confusion. Multiply by 300 on ingestion. All validation above uses the corrected value.
2. **Collection gaps.** October 2026 is missing 6 buckets. One checked (2 October 10:00 UTC) has 1,437 items in the API but none in the dump, so the dump is incomplete there. March 2021 is missing 57 buckets; one checked (10 March 15:00 UTC, inside a 37-bucket hole) is also empty in the API, so that one is a real source outage.
3. **Column types vary by month.** Price columns are int32, int64 or double depending on the month, and volumes int32 or int64; readers must merge schemas (`union_by_name`). The hidden `month` partition key reads as text ("03") in one file and a number ("10") in another unless partition types are fixed.
4. **`items.parquet` is not the GE mapping.** It has 17,964 rows (tradeable and untradeable) and no missing buy limits, whereas the Wiki's `/mapping` has 510 GE items without a limit. Do not use it for buy limits; keep `/mapping` snapshots (ADR-0005).

## Effect on the M2 plan

| | Plan in ADR-0005 | With the dump |
|---|---|---|
| Hourly history since March 2021 | about 49,000 Wiki requests | derived from the dump's 5-minute data (exact roll-up) |
| 5-minute history | from July 2026 only, about 32,000 requests | **all of it**, from March 2021 |
| Wiki requests for history | about 81,000 | about 136 for validation (2 random hours per month against `/1h`) plus gap fills (dozens to low thousands) plus everything after 17 October 2026 |
| Download | Wiki, at most 1 request a second | Kaggle, 68 files, 4.8 GB, paced |

Storage, given ADR-0015's 6 GB stop: the full 5-minute archive (4.8 GB) stays on local disk, as backfill raw data already does. R2 holds the hourly history derived from it plus a rolling window of recent 5-minute data.

## Recommended ingestion rules (if adopted)

- Pin the source: dataset version 1, with a SHA-256 for every file recorded in the manifest. A future version 2 is a new source, validated again.
- Convert `timestamp` × 300, unify types, and tag every row with its provenance (`kaggle:antonygarand/osrs-ge-5m@v1` or `wiki_api`) and measurement regime (ADR-0006).
- For each month, reconcile 2 randomly chosen hours against the Wiki `/1h` endpoint; any mismatch quarantines the month and falls back to the API for it.
- Fill missing buckets from the API using the existing gap-finder (expected buckets minus stored buckets). Buckets the API also returns empty are recorded as source outages.
- Attribute the dump's author alongside the OSRS Wiki in published data (CC BY-NC-SA 3.0).
- Courtesy: let the author know about the timestamp and README discrepancy (Sylvester's call).
