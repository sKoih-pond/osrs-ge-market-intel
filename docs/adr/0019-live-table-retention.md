# ADR-0019: Live tables keep ideas for ever and the rest for 30 days; closed months in zstd; fewer checkpoints

- **Status:** Accepted on 24 October 2026: all four rules (A1, B1, C1, D1), switched on in the same pull request. The first weekly run to apply them is Monday 26 October 00:xx UTC (rule D from the first commit after merge); the first rows are deleted by the run of Monday 23 November (days before 24 October), and October's 5-minute month is rewritten by the run of 9 November.
- **Date:** 2026-10-21
- **Decided by:** Proposed by the agent within the M2 preparation scope (21 October 2026); accepted by Sylvester ("yes, switch on ADR-0019", 24 October 2026). Amends ADR-0015 (note added) and refines ADR-0016 decision 15.

## Context

ADR-0015 budgets R2 storage at "under 4 GB in year one", halts every run at 6 GB, and M2's acceptance criterion A5 caps storage at 5 GB. The live tables, written only by the hourly workflow, grow faster than that budget allows.

### Measured

Stored bytes as the cost guard reads them before every hourly run (`guard` line, `storage_gb`, in the `hourly` workflow logs):

| UTC | GB | UTC | GB |
|---|---|---|---|
| 20 Oct 02:21 | 0.021 | 21 Oct 02:27 | 0.056 |
| 20 Oct 14:07 | 0.038 | 21 Oct 06:27 | 0.061 |
| 20 Oct 14:27 | 0.038 | 21 Oct 08:07 | 0.063 |
| 20 Oct 18:27 | 0.044 | 21 Oct 10:27 | 0.067 |
| 20 Oct 22:27 | 0.049 | 21 Oct 12:27 | 0.069 |
| 20 Oct 23:07 | 0.050 | 21 Oct 14:27 | 0.072 |
| | | 21 Oct 16:27 | 0.075 |
| | | 21 Oct 18:27 | 0.078 |

A least-squares line through all 14 readings gives **34.0 MB a day**, steady so far (a quadratic term is indistinguishable from zero at the readings' 1 MB resolution). Every reading predates the first full weekly maintenance since go-live, due Monday 26 October 00:xx UTC (the 20 October run compacted tables that were then a day old). **How much compaction and VACUUM take back is still pending** (see "Measurements still to come").

Growth per table, measured on recorded full-size responses (one hourly run on a scratch lake, 19 October):

| Table | Per day | Note |
|---|---|---|
| `gold/market_snapshot` | 7.9 MB | 0.33 MB an hour: 4,250 rows, 38 columns, 78 bytes a row |
| `silver/prices_5m` | 5.3 MB | 517,656 rows at 10.3 bytes a row, snappy, sorted by bucket; 7.74 bytes a row in zstd sorted by item |
| `silver/latest_snapshots` | 2.9 MB | 4,540 rows an hour, used only for the staleness input |
| `bronze/` | 5.9 MB | expires after 30 days (R2 lifecycle rule, ADR-0015): about 0.18 GB steady |
| `silver/item_dim`, `silver/wiki_5m_buckets`, `ops/task_runs` | small | |

Of the mart's rows, 683 to 1,156 an hour are ideas (`is_idea`: grade A or B with a positive net GP), 16 to 27% and 20% on average over four hours of 20 and 21 October; the page publishes the top 100 (2.4%).

That is about 22 MB a day of data and bronze. The other 12 MB a day are Delta log and checkpoint files, and hourly files that compress worse than the bulk-written day measured on 19 October.

### Since the proposal (added on acceptance, 24 October 2026)

| Readings (`storage_gb`) | Live growth |
|---|---|
| 20 and 21 October (above) | 34.0 MB a day |
| 22 Oct 06:06 to 23 Oct 18:27: 1.469 to 1.535 GB | about 44 MB a day |
| 23 Oct 18:40 (after the second history upload, about 1.753 GB) to 24 Oct 13:27: 1.794 GB | about 52 MB a day |

Daily growth rose as the checkpoint model predicted ("towards about 50 to 55 MB by 25 October"), so rule D matters as projected. The M2 history upload is done: 1.54 GB of `history/` tables on 22 and 23 October (`docs/plans/M2.md`), 0.06 to 0.36 GB less than the projection assumed, and the rules take effect from 26 October instead of 9 November. Both move the dates in the projection below slightly later; the 2 November reading will refine them.

### Found by replaying the writers (`docs/research/probes/log_growth.py`)

The probe replays a week of hourly commits on a local lake through `Lake.write`, with full-size row counts and schemas and synthetic values, then the weekly maintenance.

1. **Checkpoints grow with commits times files.** `Lake.write` checkpoints after every commit (spike S1 rule 2). Each checkpoint lists every live file with its column statistics, delta-rs 1.6.6 writes it uncompressed, and `cleanup_metadata` keeps log files for delta's default 30 days. After one week of hourly commits the probe held 163 MB of data and **140 MB of checkpoints**; the daily increment grew from 4.7 to 35 MB across the week. Per live file a checkpoint costs about 2.7 KB for the mart (38 columns), 0.9 KB for the 5-minute prices and `/latest`, 1.1 KB for `ops/task_runs` (two commits an hour) and 0.6 KB for the bucket manifest. Partitioned tables keep at least one file per day after compaction, so each checkpoint grows with the table's age; with the 30-day log, the model puts checkpoints at about 1.7 GB a year from go-live with no change.
2. **Compaction does not free space the day it runs.** It replaces the hourly files, and VACUUM deletes the replaced files only once they have been removed for 7 days. Expect storage to step up on 26 October and the take-back at the 2 November run.
3. **Compaction may shrink the data itself.** On the probe's synthetic rows, a compacted day was 37% (prices) to 55% (mart) smaller than its 24 hourly files. Real data will differ; the 2 November reading will tell.

The model is consistent with the measured rate so far (it gives 25 to 31 MB a day over 20 and 21 October against 34 measured, the gap fitting hourly files that compress worse than the bulk-written day), but it rests on synthetic statistics. Its testable prediction: **without a change, daily growth rises from 34 MB towards about 50 to 55 MB by 25 October**, as files accumulate since the 20 October compaction.

### Projection

`docs/research/probes/storage_projection.py`, from the measured 0.078 GB on 21 October, with the M2 history upload of 1.6 to 1.9 GB (`docs/plans/M2.md` section 8) on about 15 November, data at the hourly-file sizes above (an upper bound), bronze steady at 0.18 GB, about 0.15 GB of replaced files waiting for VACUUM, and the checkpoint model. Rules A to D are defined below; rules take effect from the weekly run of 9 November.

| Scenario | 4 GB | 5 GB (M2 A5) | 6 GB (halt) | GB on 31 Oct 2027 |
|---|---|---|---|---|
| 34 MB a day, unchanged | 20 to 29 Dec 2026 | 18 to 27 Jan 2027 | 17 to 26 Feb 2027 | 14.4 to 14.7 |
| No change (model) | 23 Dec 2026 to 8 Jan 2027 | 13 to 28 Feb 2027 | 5 to 21 Apr 2027 | 9.8 to 10.1 |
| Rules A, B, C | 4 Feb to 13 Mar 2027 | 6 Jun to 12 Jul 2027 | 5 Oct to 10 Nov 2027 | 5.9 to 6.2 |
| Rules A (published ideas only), B, C | 19 Feb to 4 Apr 2027 | 15 Jul to 28 Aug 2027 | 8 Dec 2027 to 21 Jan 2028 | 5.4 to 5.7 |
| Rule D only | 1 to 19 Feb 2027 | 2 to 21 Apr 2027 | 2 to 20 Jun 2027 | 8.2 to 8.5 |
| **Rules A, B, C, D (recommended)** | 10 Jun to 31 Jul 2027 | 26 Nov 2027 to 16 Jan 2028 | 13 May to 3 Jul 2028 | 4.6 to 4.9 |

Earlier dates go with the 1.9 GB upload. With no change, M2's own 5 GB limit falls in February 2027, three months after the upload; ADR-0015's "under 4 GB in year one" fails in every scenario that includes the history upload. On 31 October 2027 the recommended rules leave about 2.5 GB of live data (against 6.1 GB unchanged) and 0.15 GB of checkpoints (against 1.7 GB).

## Options

**A. `gold/market_snapshot` (the mart and the paper record)**

| | Keeps | Lasting growth |
|---|---|---|
| A0, today | every hour's full mart | 7.9 MB a day |
| **A1** | every idea row (`is_idea`) for ever, the full mart for 30 days | about 1.6 MB a day (1.3 to 2.1) plus about 0.2 GB steady |
| A2 | only published ideas (`idea_rank` 100 or better) for ever, the full mart for 30 days | about 0.2 MB a day plus about 0.2 GB steady |
| A3 | the full mart for 90 days, ideas for ever | as A1, plus about 0.4 GB steady |

A1 and A2 both satisfy AT7, which scores published ideas only. A1 also keeps the ideas below the publication cut, which the M4 simulator can use to test the cut itself; it costs about 0.5 GB a year more than A2.

**B. `silver/latest_snapshots`:** B0 keep everything (2.9 MB a day); **B1** keep 30 days (about 0.1 GB steady); B2 keep 7 days. The table feeds only the staleness input of the current hour; 30 days matches bronze, so the hour's raw `/latest` and its typed rows expire together.

**C. `silver/prices_5m`:** C0 as written; **C1** rewrite each closed month in zstd, each day sorted by item (25% smaller, same rows); C2 later, move 5-minute months older than a year out of R2 to a free private host (a private Hugging Face dataset, ADR-0020) and keep their hourly rollup in R2 (`docs/plans/M2.md` section 7.5). The 5-minute series itself is kept for ever in R2 under C1.

**D. Checkpoints:** D0 one per commit (today); **D1** one every 10th table version, which S1 rule 2 already allows ("or about 10"); D2 a 7-day `delta.logRetentionDuration`, which needs a table property against the S1 baseline of none.

## Decision (proposed)

Recommended: **A1, B1, C1 and D1**. A2 is the cheaper choice if keeping only what was published is enough.

### How the rules run

- **Inside the weekly `maintain` task**, which the hourly workflow runs inside its `ge-intel-lake-writer` concurrency group, so the tables keep one writer. Retention runs first, then compaction, VACUUM (7 days, full), log clean-up and a checkpoint, as before.
- **Deletes go through delta-rs** (`Lake.delete`: one commit, then a checkpoint). No R2 lifecycle rule touches a table: it would delete files the Delta log still lists. The files the rules replace are deleted by the VACUUM a week later, as compaction's are.
- **A1:** rows of days older than 30 days whose `is_idea` is not true (`IS NOT TRUE`, because a grade A or B row without a positive net GP has `is_idea` null) are deleted; each touched day's file is rewritten with its idea rows. Days are kept at least 30 and at most 37 days in full. The run looks at the day partitions that crossed the limit during its week plus four weeks before (`maintain.SWEEP`), so a paused week catches up and the scan stays at five weeks of day files; a longer pause takes one manual run with an earlier `--start` (runbook).
- **B1:** whole days older than 30 days are dropped by a predicate on the partition column only, so no file is rewritten.
- **C1:** a month is closed once the live gap-fill can no longer write to it (its last day plus `tasks.LIVE_GAP_WINDOW`, 7 days; a test keeps `maintain.CLOSED_AFTER` in step). The oldest closed month not yet in its final form (one zstd file a day) is rewritten in one commit, one month a run; a month that later gains a file from a backfill is rewritten again. Measured locally at full size: 1.8 seconds a day, about a minute a month, peak memory about 0.6 GB.
- **D1:** `Lake.write` and `Lake.delete` checkpoint when the new version is a multiple of `lake.CHECKPOINT_EVERY`; the weekly maintenance still checkpoints every table. A reader replays about nine commit files at most after the latest checkpoint.
- **`history/` is never touched** (ADR-0018): the rules act on `maintain.RETAINED` only, and a test checks that it shares no table with `history.TABLES`.
- **The published v1 contract does not change:** the rules act only on days older than 30 days and on file layout, and `publish` reads the current hour (tested: `snapshot.json` and `status.json` are byte-identical before and after).

### Before acceptance (until 24 October 2026)

`maintain.RETENTION` is `KEEP_ALL` and `lake.CHECKPOINT_EVERY` is 1, so the code changes nothing on merge. Each weekly run then:
- reports, per rule, what the proposal would remove (days, files, bytes and rows in scope, from the Delta log only, no data read);
- reports every table's live files and bytes after compaction, so storage can be traced per table without R2 credentials.

Both land in the `maintain` task's detail in `ops/task_runs` and in the run log.

### Turning it on

In one pull request that marks this ADR Accepted (or Accepted in part): set `maintain.RETENTION = PROPOSED` (or a `Retention` with only the chosen rules; A2 would need its predicate changed to `idea_rank <= 100`) and `lake.CHECKPOINT_EVERY = 10`. The next Monday run applies them; the run after it logs that run's Class A and B operations (`previous_window`). **Done on 24 October 2026**, all four rules.

### Cost (ADR-0015)

Deletes are free operations; rewrites are Class A writes. Estimates per weekly run once on: A1 rewrites about seven day files plus a commit and a checkpoint (about 15 Class A); B1 writes only a commit and a checkpoint (about 3 Class A); C1, once a month, writes about 31 day files plus a commit and a checkpoint (about 40 Class A, about 0.12 GB written) and reads the month (about 0.17 GB, about 100 Class B); the dry-run reports read nothing extra. A normal hourly run measured 85 Class A and 555 to 703 Class B (`previous_window`, 20 and 21 October), against per-run caps of 500 and 5,000. D1 saves nine checkpoint writes in ten and adds up to nine small reads per table load. Engine writes are not counted by `RunBudget` (compaction's are not either); they are bounded by the step timeout and checked by the next run's `previous_window`.

## Consequences

- **The paper record:** every idea is kept as recorded, for ever. Rows that were not ideas (grade C, or no positive net GP) older than 30 to 37 days are gone and cannot be rebuilt, because `/latest` and bronze expire after 30 days too. Each hour's counts survive in `ops/task_runs` (the `build_snapshot` detail: rows written, ideas, grades).
- **AT7 is unaffected:** an idea is scored from its own row and `silver/prices_5m`, which is kept for ever.
- **Delta time travel stays 7 days** (ADR-0015).
- **Even with A to D, 5 GB arrives around December 2027 and the 6 GB halt around mid-2028.** The next levers are C2 and whatever compaction is measured to save; this ADR is revisited with the 2 November reading and again before storage reaches 4 GB.
- **Fewer checkpoints** cost a little more reading per table load (up to nine commit files), well inside the Class B allowance.
- The weekly report adds per-table bytes to `ops/task_runs`, which ADR-0015's guard alone could not give.

## Measurements still to come

| When | Reading | What it decides |
|---|---|---|
| 22 to 25 Oct | daily growth from the hourly `guard` lines | towards 50 to 55 MB a day: the checkpoint model holds and D1 matters as projected; near 34: checkpoints are smaller than modelled. **Measured: about 44, then 52 MB a day; the model holds** |
| 26 Oct, first weekly run | a step up, not down (compacted copies beside the hourly files) | confirms finding 2 |
| 2 Nov, next weekly run | the take-back; growth from 2 to 9 Nov is the lasting rate | replaces the hourly-file upper bounds in the projection |
| first run after this code merges | per-table files and bytes, and what each rule would remove | sizes A1 to C1 on the real lake |

## Alternatives considered

- **An R2 lifecycle rule on `gold/` or `silver/`:** would delete files the Delta log still lists, breaking the tables, and would be a second writer. Rejected; the only lifecycle rules stay the ADR-0015 ones on `bronze/` and incomplete uploads.
- **Ideas in a separate `gold/paper_record` table, with whole-day drops on the mart:** no rewrites, but an extra commit every hour and a union for readers of recent ideas. Kept as the fallback if the weekly rewrite ever costs too much.
- **Scanning every old day each week instead of a bounded sweep:** simpler, but each run's reads would grow with the table's age.
- **A separate monthly task for C1:** one more cadence; the weekly run with one month per run does the same.
- **A shorter VACUUM retention:** small gain, and time travel would drop below S1's 7 days.
- **A 7-day log retention property (D2):** a table property against the S1 baseline; D1 gets most of the saving without one.

## References

- Code: `src/ge_intel/maintain.py` (`Retention`, `apply_retention`), `src/ge_intel/lake.py` (`Lake.delete`, `CHECKPOINT_EVERY`). Tests: `tests/test_retention.py`, `tests/test_lake.py`, `tests/test_e2e.py`.
- Evidence: `docs/research/probes/log_growth.py`, `docs/research/probes/storage_projection.py`; spike S1 (`docs/spikes/S1_results.md`, rule 2); `docs/plans/M2.md` section 8; `docs/kickoff/acceptance_tests.md` (AT7).
