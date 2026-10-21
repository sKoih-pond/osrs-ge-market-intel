# ADR-0018: History has its own tables and writer; the live gap-fill looks back 7 days at most

- **Status:** Proposed (accepted when Sylvester merges the M2 preparation PR)
- **Date:** 2026-10-22
- **Decided by:** Agent, within the M2 planning scope set by Sylvester; measured where noted. Amends ADR-0005 and ADR-0016 (each carries a note).

> **Where it runs, amended by ADR-0020 (Proposed, 2026-10-21):** the history job runs on a hand-started GitHub runner (`history` workflow, its own `ge-intel-history-writer` group) instead of the laptop, which stays possible. Decision 4 holds on the runner's disk: each run rebuilds every `history/` table from the pinned dump and the raw responses; the raw responses persist between runs in `history/raw_wiki` in R2, an input that `build` never writes and `upload` never copies.

## Context

M2 loads history from the Kaggle dump `antonygarand/osrs-ge-5m` v1 (ADR-0005, amended), fills its gaps from the Wiki API and joins it to the live series M1 has recorded since 18 October 2026 23:00 UTC. Two designs that look natural are traps.

1. **The gap-finder would turn into a backfill.** `tasks.interval_for` gives the hourly `ingest_5m` task an interval that starts at the first bucket in `silver/wiki_5m_buckets` (ADR-0016, decision 5). If history landed in that manifest, the first bucket would become 8 March 2021 07:35, about 590,000 buckets would count as expected, and every hourly run would start fetching years of buckets from the Wiki, 96 at a time, with nobody having started a backfill. The delegation plan reserves backfills for Sylvester. Even without history in the manifest, a long pause has the same effect on a smaller scale: three weeks off would mean days of unattended catch-up.
2. **The seam.** The dump ends at the bucket starting 17 October 2026 13:10 UTC; the live manifest starts at 18 October 23:00 (confirmed in the public `status.json`: 12 buckets on 18 October, 768 of 768 recorded to 21 October 15:00). Between them lie 405 buckets (33 hours 45 minutes) that only the Wiki API can supply, at 1 request a second.

A third constraint shapes both: one writer per table (kickoff decision 8). The hourly workflow writes the live tables inside its `ge-intel-lake-writer` concurrency group. The history job runs by hand on the laptop, because the dump and the backfill's raw responses stay on local disk (ADR-0015), so it cannot join that group.

## Decision

### 1. History lives in `history/`, written only by the history job

| Prefix | Written by | Tables |
|---|---|---|
| `bronze/`, `silver/`, `gold/`, `ops/` | the hourly workflow only | unchanged from M1 |
| `history/` | `ge_intel.history`, started by hand, only | `prices_1h`, `prices_5m`, `buckets_5m`, `months` |

- The live gap-finder reads only `silver/wiki_5m_buckets`, so history cannot reach it.
- Enforced in code, not by convention: `history.write` refuses any table outside `history/`; tests check that no registry task and no weekly maintenance step (VACUUM and compaction are writes) touches a history table.
- History covers [8 March 2021 07:35, `LIVE_START`) and live covers [`LIVE_START`, now), with `LIVE_START` = 18 October 2026 23:00 UTC. Readers join the two at that instant. The upload refuses to run if the live manifest in R2 does not start exactly there, so the series can neither overlap nor leave a hole.

### 2. The scheduler heals the live series 7 days back, no further

`tasks.LIVE_GAP_WINDOW = 7 days`. The gap-fill interval is now `max(min(first live bucket, end - 24 h), end - 7 days)` to `end`. A young lake behaves as before.

- Worst case: 2,016 buckets, fetched newest first at 96 a run and two runs an hour, so about 11 hours of catch-up at roughly 3 requests a minute, while the current hour publishes from the first run.
- Why 7 days: it matches the VACUUM retention and the weekly maintenance cycle; the watchdog opens an issue after 2 hours of staleness, so a gap older than a week is one a person already knows about; and anything older is a backfill, which a person starts.
- Older live gaps are filled on purpose with `ge-intel run ingest_5m --start <UTC> --end <UTC>`, which is not clamped (96 buckets an invocation), run from the laptop with the `hourly` workflow disabled meanwhile so there is still one writer at a time (runbook). `ge-intel gaps --start` lists them.

### 3. The seam is filled by the history job, not the live path

`ge-intel history fill` fetches every bucket between the dump's first bucket and `LIVE_START` that neither the dump nor an earlier fill holds, newest first, so the seam comes first. That is the 405 seam buckets plus the dump's own gaps, in one mechanism. Responses are kept on local disk with their request metadata (ADR-0015); an empty response is kept as the evidence that a gap is a source outage. At 1 request a second the seam takes about 7 minutes. The hour of 17 October 13:00 is built from both sources and its rows say `source = mixed`.

### 4. Inputs on disk; every history table is derived

The only inputs are the pinned dump files and the raw Wiki responses the job stored. `ge-intel history build` rebuilds every `history/` table from them, deterministically: deleting the local lake and rebuilding reproduces identical content hashes (tested). Two different bodies stored for one bucket stop the build for a person to decide (a revision).

### 5. The `/1h` check passes within the source's rounding, not 0.1% alone

A checked hour passes when the item sets match, both volumes match exactly, and each price is within the larger of 0.1% and 0.02 GP (decimal months) or 1.01 GP (months the dump stores as integers). Measured on 27 real hours (87,631 item-hours, 14 to 19 October 2026): item sets equal in every hour, volumes exact on both sides, every price within 0.01 GP of `/1h`, but 201 of 146,493 priced item-sides fail a 0.1% rule alone, all cheap items where one rounding step exceeds 0.1%. The tolerance is three roundings (the 5-minute averages, this job's hourly VWAP, the API's own hourly average) plus float slack.

### 6. Hourly VWAPs are rounded to 0.01 GP and history tables are zstd

The API publishes its averages to 0.01 GP. Rounding the rolled-up VWAP to the same precision made the hourly table 18% smaller on real data (9.64 against 11.75 bytes a row) and matches `/1h` to within 0.01 GP. History tables use zstd (about a fifth smaller than delta-rs's default snappy); live tables are unchanged.

### 7. Check hours are drawn from the dump alone

Each month's two check hours are the month's complete dump hours (all 12 buckets from the dump) with the lowest SHA-256 of (source, month, file hash, hour). The pick is reproducible, as good as random, and does not move when a fill changes anything else, so the requests already made stay valid.

## Consequences

- The live status page keeps counting live buckets only. History completeness is reported by `history/months` and `history/buckets_5m`.
- A live gap older than 7 days now waits for a person (runbook). M1's acceptance test (a skipped run is filled by the next run) is unaffected.
- Readers that need one continuous series need a union at `LIVE_START`; the dbt models for it are an M2 follow-up (`docs/plans/M2.md`).
- The history job is one more writer of R2, outside the concurrency group. It writes only `history/`, which nothing else writes, so the two never meet on a table; its own runs are by hand and one at a time.

## Alternatives considered

- **History in `silver/prices_5m` and `silver/wiki_5m_buckets`** (one table per grain): simplest to read, but it is the trap above, makes two writers of live tables, and puts years of history under the weekly VACUUM and the status page's completeness figure.
- **Only bounding the gap-fill:** stops the backfill but still mixes ownership and the completeness figure.
- **Only separate tables:** a long pause would still make the hourly run backfill weeks of buckets unattended.
- **Filling the seam through the live path** (`ge-intel run ingest_5m` dispatched about five times): works, but puts backfill raw responses in R2 against ADR-0015, moves the live start earlier than go-live, and still needs a second mechanism for the dump's gaps.
- **A strict 0.1% price tolerance:** fails on rounding alone (decision 5).
- **Full-precision hourly VWAPs:** about 0.25 GB more in R2 for differences below the source's own precision.

## References

- `docs/plans/M2.md` (the plan, evidence and execution order); `docs/research/2026-10-20_kaggle_osrs_ge_5m_assessment.md`.
- Code: `src/ge_intel/history.py`, `src/ge_intel/kaggle.py`, `src/ge_intel/tasks.py` (`LIVE_GAP_WINDOW`). Tests: `tests/test_history.py`, `tests/test_kaggle.py`, `tests/test_tasks.py`.
