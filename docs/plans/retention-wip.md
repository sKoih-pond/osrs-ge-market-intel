# Retention work in progress (temporary; folded into ADR-0019 and removed before the PR opens)

Branch `claude/busy-cray-yztp0u`, based on `origin/m2/historical-foundation` (PR #10). Paused 21 October 2026 about 17:10 UTC.

## 1. Storage readings (`guard` line, `storage_gb`, hourly workflow logs)

| UTC | storage_gb | Run (job id) |
|---|---|---|
| 20 Oct 02:21 | 0.021 | 39358438854 (118418645356), first run; ran `maintain` for 12 to 19 Oct on tiny tables |
| 20 Oct 14:07 | 0.038 | given |
| 20 Oct 14:27 | 0.038 | 39439255324 (118684456780) |
| 20 Oct 18:27 | 0.044 | 39474776305 (118807794477) |
| 20 Oct 22:27 | 0.049 | 39508145057 (118922191694) |
| 20 Oct 23:07 | 0.050 | given |
| 21 Oct 02:27 | 0.056 | 39533043161 (119003204204) |
| 21 Oct 06:27 | 0.061 | 39555433285 (119073995673) |
| 21 Oct 08:07 | 0.063 | given |
| 21 Oct 10:27 | 0.067 | 39584624816 (119170363758) |
| 21 Oct 12:27 | 0.069 | 39599633052 (119220558030) |
| 21 Oct 14:27 | 0.072 | 39616842669 (119280074463) |
| 21 Oct 16:27 | 0.075 | 39634822913 (119342375238) |

About 34 MB a day and flat so far. No log after the 26 October maintenance exists yet. `gh run view --log` is refused by the session proxy; the GitHub MCP `get_job_logs` (job id, `return_content`, tail about 40 lines for no-op `:26` runs) works.

## 2. Probe findings (`docs/research/probes/log_growth.py`, synthetic values, full-size rows and schemas)

- One week of hourly commits: data 163 MB (synthetic, compresses worse than real), Delta JSON 1.9 MB, **checkpoints 140 MB**. `Lake.write` checkpoints after every commit; each checkpoint lists every active file with its stats and is written **uncompressed** by delta-rs 1.6.6 (parquet-rs); `cleanup_metadata` keeps log files for delta's default 30 days.
- Daily checkpoint increment grows within a week (4.7, 9.8, 14.9, 20, 25, 30, 35 MB) until compaction cuts the file count. Per add action: about 2.7 KB (mart, 38 columns), 0.9 KB (5-minute prices, `/latest`), 1.1 KB (`ops/task_runs`, two commits an hour), 0.6 KB (bucket manifest).
- Weekly compaction does not free space the day it runs: the replaced hourly files are tombstoned now and VACUUM (7-day retention, counted from the remove) deletes them a week later. Expect a step up on 26 Oct and the take-back on 2 Nov.
- Compacted day partitions were 37% (prices) and 55% (mart) smaller than their 24 hourly files on synthetic data; real ratio unknown until 2 Nov.
- Rough steady-state model without changes: checkpoints about 3.2 MB x (table age in days + 70) plus about 0.3 GB (task_runs and manifest), so roughly 0.6 GB at 30 days and 1.7 GB at a year. Prediction to check: daily growth rises towards about 55 MB a day before 26 Oct.
- S1 rule 2 allows "checkpoint every run (or `delta.checkpointInterval` about 10)". Candidate option for the ADR: checkpoint every 10th version (about 10 times fewer checkpoint bytes); a 7-day `delta.logRetentionDuration` would need a table property, against the S1 "no properties" baseline.

## 3. delta-rs behaviour confirmed (1.6.6)

- `delete("snapshot_date < 'X' AND is_idea IS NOT TRUE")` deletes rows whose `is_idea` is null (the mart's `is_idea` is null when `net_gp` is null and the grade is A or B).
- A delete on partition columns only adds no files (metadata only, free deletes later at VACUUM).
- Files written with `WriterProperties(compression="ZSTD")` are named `*.zstd.parquet`: usable, test-pinned, as the "month already rewritten" check (one file per day partition, zstd).
- `get_add_actions(flatten=True)` returns an arro3 table; wrap with `pa.table(...)`. Stats cover the first 32 columns, so the mart's `is_idea` (column 37) has none.

## 4. Implementation plan (not started)

- `maintain.py`: `Retention` policy (frozen dataclass), `OFF` live until Sylvester accepts ADR-0019, `PROPOSED` = mart full 30 days, `/latest` 30 days, closed months of prices rewritten. Order: retention deletes, month rewrite, compaction, VACUUM, log clean-up. Each maintain run also records active files and bytes per table (metadata only) and what `PROPOSED` would remove (dry run).
- Mart: delete non-idea rows in `[start - 30 d - 28 d sweep, end - 30 d)` so the scan stays bounded; `/latest`: partition delete `< end - 30 d`; prices: oldest closed month whose end is at least `LIVE_GAP_WINDOW` before `end`, one month a run, one commit from a day-by-day stream sorted by (item, bucket), zstd.
- Never touch `history.TABLES`; contracts unchanged (test with a hand-built mart table and `publish`, no dbt); cost per run estimated against the 500 / 5,000 caps.
- Tests without dbt (DuckDB's extension host is blocked in the cloud session; the 7 e2e tests run in CI only).
- Docs: ADR-0019 (Proposed), note on ADR-0015, `docs/plans/M2.md` section 8 and 7.5, `docs/runbook.md`, ADR index.

## 5. Environment notes

- `uv` in the container is 0.11; the project needs 0.13 or later: a newer uv was installed in a scratch virtualenv.
- `extensions.duckdb.org` is denied by the environment's network policy, so dbt cannot load the `delta` extension locally.
