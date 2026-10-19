# Spike S1 results: Delta Lake on Cloudflare R2 across three engines

- **Run:** 20 October 2026, 21:10 to 22:01 UTC (inside the 1-day time box)
- **Plan:** `S1_delta_on_r2.md`. Harness: `spikes/s1/` (git-ignored). Each step logged its UTC window to `results.jsonl`, with a usage check before each step in `guard_log.jsonl`.
- **Verdict: GO for ADR-0003 (Delta Lake).** Every engine reads and writes the shared tables correctly on R2 with one table-property baseline. Six design rules follow, listed at the end.

## Versions (pins for ADR-0003 and ADR-0011)

| Component | Version |
|---|---|
| Python | 3.12.14 (Homebrew) |
| deltalake (delta-rs) | 1.6.6 |
| duckdb | 1.5.6, with the `delta` and `httpfs` extensions |
| dbt-core / dbt-duckdb | 1.12.6 / 1.11.0 |
| pyspark | 4.2.0 (Scala 2.13, bundles Hadoop 3.5.0) |
| delta-spark | 4.4.1 |
| hadoop-aws | 3.5.0 (pulls AWS SDK v2 bundle 2.35.4; 37 jars, 751 MB, cached once from Maven Central) |
| JDK | Temurin 17.0.20.1 |
| pyarrow | 26.0.0 |

## Results by step

| # | Test | Result | Evidence |
|---|---|---|---|
| 1 | delta-rs creates the table and appends 24 hourly batches | **Pass** | 24 versions (0 to 23); 79,949 rows written and read back |
| 2 | Replace one hour twice with replace-where | **Pass** | Hour hash unchanged (`b7d4abb19b9e0c9e` before and after both runs); total rows unchanged; versions 24 and 25 |
| 3 | 8 writers append at the same instant | **Pass** | All 8 committed exactly once, as versions 26 to 33; each batch exactly 100 rows; delta-rs resolved the races itself |
| 4 | Control: write without `aws_conditional_put` | **Not refused** (differs from the delta-rs docs) | The write was accepted |
| 4b | Control: 8 racing writers without `aws_conditional_put` | **Safe** | All 8 landed exactly once (versions 1 to 8). delta-rs 1.6.6 appears to use conditional writes by default; docs are out of date |
| 5 | Writer dies after uploading data, before committing | **Pass** for readers | Version and rows unchanged (33; 80,749): the orphan is invisible |
| 5b | Clean-up finds the orphan | **Pass only in full mode** | `vacuum(dry_run=True)` lists 2 files, not the orphan; `vacuum(..., full=True)` lists 3, including it |
| 6 | DuckDB reads over R2 | **Pass** | Rows and both volume sums identical to delta-rs (80,749; 1,309,879,147; 1,088,770,410); time travel to version 5 gives 20,012 rows, matching delta-rs; a partition filter opened only the 8 matching files |
| 7 | dbt-duckdb model over the Delta table; gold write paths | **Pass** | `dbt run` PASS=1 (79,949 rows); delta-rs overwrite twice gave identical hashes |
| 7b | DuckDB `INSERT` into Delta | **Works, not idempotent** | Needs `ATTACH ... (TYPE delta, READ_ONLY false)`; a re-run doubled the table (79,949 to 159,898 rows) |
| 8 | Spark reads, appends, compacts, vacuums over S3A | **Pass** | Read 80,749; appended 25; compaction removed 34 files and added 3; Spark vacuum dry run listed 37; afterwards delta-rs and DuckDB both read 80,774 |
| 9 | Table-feature compatibility | **Pass** | Protocol stayed at reader 1 / writer 2 with no features and no properties after delta-rs, DuckDB and Spark writes and Spark compaction |
| 10 | Cost of one hourly run | **Measured** (below) | Batching per table is about 7x cheaper than per-bucket commits |
| 11 | Analytics latency | **Measured** | A minute's operations first appear about 1.5 minutes later and are complete within about 5 minutes |

## Configuration that worked (no secrets)

- **delta-rs:** `storage_options = {AWS_ENDPOINT_URL: https://<account>.r2.cloudflarestorage.com, AWS_REGION: auto, aws_conditional_put: etag, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY}`.
- **DuckDB:** `CREATE SECRET (TYPE s3, KEY_ID, SECRET, REGION 'auto', ENDPOINT '<account>.r2.cloudflarestorage.com', URL_STYLE 'path')`; read with `delta_scan('s3://...')`; time travel with `ATTACH 's3://...' AS t (TYPE delta)` then `SELECT ... FROM t AT (VERSION => n)`.
- **dbt-duckdb:** the same secret in `profiles.yml` (`secrets:` block with `env_var` values), `extensions: [httpfs, delta]`.
- **Spark:** `fs.s3a.endpoint=https://<account>.r2.cloudflarestorage.com`, `fs.s3a.endpoint.region=auto`, `fs.s3a.path.style.access=true`, `SimpleAWSCredentialsProvider`; Delta extension and catalog; `retentionDurationCheck.enabled=false` only for dry runs. `JAVA_HOME` must be JDK 17 and `PYSPARK_PYTHON` must point to the project interpreter (Spark otherwise started workers on the system Python 3.10 and failed).

## Cost measurements (Cloudflare analytics, per-minute, bucket `ge-intel-spike`)

**Whole spike:** about 844 Class A and 5,519 Class B operations account-wide (0.08% and 0.06% of the monthly free allowance; 4% and 11% of the spike's own cap). Deletes were free.

**Per step (selected)**

| Step | Class A (writes and lists) | Class B (reads and heads) | Note |
|---|---|---|---|
| 1: 24 appends, no checkpoint | 145 | 722 | Reads grow with each commit: every append replays the log since the last checkpoint |
| 3: 8 racing writers | 136 | 692 | 46 write attempts for 8 commits: each lost race is a billed write plus a log re-read |
| 5: orphan via pyarrow | 3 multipart calls for one small file | | pyarrow's S3 writer uses multipart upload by default |
| 8: Spark append, compact, vacuum, read | 66 | 872 | Many `HEAD` calls, and copy-then-delete pairs from the S3 connector's rename |

**Step 10, one simulated hourly run (3,372 rows, 12 five-minute slices, checkpoint after each run, then a full read)**

| Variant | Class A per run | Class B per run | Wall time |
|---|---|---|---|
| A: 12 separate commits | 77 to 78 | 242 to 444 | 26 to 37 s |
| B: one batched commit | 9 to 11 | 14 to 49 (grows with the full read) | 5 to 9 s |

Projection for variant B across about 8 tables per run and 744 runs a month: roughly 65,000 Class A (6.5% of free) and 300,000 Class B (3%), inside ADR-0015's halt thresholds. The per-run caps of 500 Class A and 5,000 Class B leave 5 to 10 times headroom over measured use.

## Observations on the controls themselves

- **Analytics rate limit.** Cloudflare's GraphQL analytics allowed about six month-wide multi-dataset queries in six minutes before refusing ("budget depleted"); the limit is cost-based and shared with dashboard browsing. The guard failed closed as designed and the spike waited five minutes. One query per hourly run is well inside the limit.
- **Storage analytics lag.** The storage dataset still reported 0 objects for the spike bucket at the 21:10 snapshot long after data was written; it updates slowly. The guard's storage check must be paired with the pipeline's own record of bytes written (ADR-0015 layer 4 already caps bytes per run).
- **Every dashboard visit costs operations.** `ListBuckets` calls from browsing the R2 dashboard showed up (11 this month).

## Design rules adopted from this spike

1. **One commit per table per run.** Batch all buckets of a run into one write; never commit per bucket.
2. **Checkpoint every run** (or set `delta.checkpointInterval` to about 10). Without checkpoints, read cost per write grows with commit count.
3. **Gold tables are written by delta-rs with overwrite or replace-where**, never by DuckDB `INSERT`, which is append-only and not idempotent.
4. **Scheduled clean-up uses `vacuum(full=True)`** with the 7-day retention, so crashed writers' files are removed.
5. **Keep the protocol baseline at reader 1 / writer 2 with no table features.** Create tables with delta-rs. Do not enable deletion vectors or column mapping without a new ADR.
6. **Set `aws_conditional_put: etag` explicitly**, even though delta-rs 1.6.6 behaved safely without it. One writer per table remains the rule (ADR-0003); racing writers are safe but waste billed operations.

Also: the ADR-0015 post-run check should inspect the previous run's window at the start of the next run, because analytics take about 5 minutes to settle.

## Clean-up

- Spike objects in `ge-intel-spike` are deleted automatically by its 7-day lifecycle rule; the empty bucket can then be removed.
- The R2 key used here is the pipeline key, so it is kept.
- `spikes/s1/` (virtualenv and harness) stays local and git-ignored until these results are reviewed.
