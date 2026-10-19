# Spike S1: Delta Lake on Cloudflare R2 across three engines

- **Decides:** ADR-0003 (table format), version pins for ADR-0011 (Spark)
- **Time box:** 1 working day. Stop at the time box and write up what is known.
- **Status:** Planned. Not started: needs the prerequisites below.

## Question

Can delta-rs (hourly writer), DuckDB and dbt-duckdb (transforms and reads) and Spark (backtest grid and reprocessing) share Delta tables in R2 safely, with one table-property baseline that all three can read and write?

## What is already known (checked 2026-10-20)

| Fact | Source |
|---|---|
| R2 evaluates `If-None-Match` and `If-Match` on `PutObject` atomically; concurrent creates of the same key yield one success and `412` for the rest. | R2 S3 API compatibility page |
| delta-rs supports R2 natively; `aws_conditional_put: 'etag'` gives safe concurrent writes without DynamoDB. | delta-rs docs, `s3-like.md` |
| Without a locking mechanism on plain S3, delta-rs refuses unsafe writes unless `AWS_S3_ALLOW_UNSAFE_RENAME=true`. | delta-rs docs, `s3.md` |
| DuckDB's Delta extension supports `INSERT` and time travel; not `UPDATE`, `DELETE` or `MERGE` (May 2026). | DuckDB blog, 7 May 2026 |
| Spark's Delta log store on S3-compatible storage: **not yet verified**. Assume single-writer only until shown otherwise. | To test |

## Prerequisites (Sylvester)

1. R2 enabled on the Cloudflare account, with a usage notification set.
2. A temporary bucket `ge-intel-spike`, created with the ADR-0015 guardrails: Standard class, no public access (no r2.dev URL, no custom domain), lifecycle rules that expire every object after 7 days and abort incomplete uploads after 1 day.
3. An R2 API token limited to `ge-intel-spike`, Object Read and Write. Exported in the shell session only (`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`), never written to a file.
4. For the Spark part: JDK 17 (for example Homebrew `openjdk@17`), installed with your approval. The Mac has Java 8, which Spark 4.x does not support. If you'd rather not install it, the Spark part waits for the repo and runs in a hand-started Actions workflow.

## Set-up

- A throwaway virtualenv in `spikes/s1/` (Python 3.12) with `deltalake`, `duckdb`, `pyarrow`, `dbt-duckdb`, `pyspark` and the matching `delta-spark`. Exact versions recorded in the write-up.
- Test schema mirrors the planned 5-minute fact table: `item_id INT32`, `bucket_start TIMESTAMP (UTC)`, `avg_high DOUBLE`, `high_vol INT64`, `avg_low DOUBLE`, `low_vol INT64`, `regime_id STRING`, `ingested_at TIMESTAMP`, `batch_id STRING`, partitioned by `bucket_date`.
- Data: the 24 hourly buckets already saved in discovery (`/1h` for 18 Oct 2026), plus synthetic rows for load tests. No new API calls are needed.

## Steps

| # | Test | Pass condition |
|---|---|---|
| 1 | delta-rs creates the table in R2 with `aws_conditional_put: 'etag'`, appends 24 hourly batches. | 24 versions; row count equals source; DuckDB reads identical totals. |
| 2 | Idempotent re-run: overwrite one hour with a predicate (replace-where) using the same input, twice. | Second run leaves the hour's rows unchanged (same content hash of the hour's rows). |
| 3 | Concurrency: 20 processes append at once, each retrying on commit conflict. | All 20 commits land exactly once; no lost or duplicated rows; log versions contiguous. |
| 4 | Control: repeat 3 without `aws_conditional_put`. | Write is refused (documents why the setting matters). If it is accepted, record it as a hazard. |
| 5 | Kill a writer after data files are written but before the commit. | Readers see the previous version; the orphan files are listed by `vacuum` dry run. |
| 6 | DuckDB reads via an R2 secret: current version, time travel to version N, filter pushdown on `bucket_date`. | Correct results; partition pruning visible in `EXPLAIN ANALYZE`. |
| 7 | Gold write path: a tiny dbt-duckdb model reads the table and its result is written to Delta by (a) DuckDB `INSERT` into a Delta table and (b) delta-rs from Arrow. | Pick the simpler path that keeps idempotency (expected: (b), because re-runs need replace-where). |
| 8 | Spark reads the delta-rs table over S3A (`fs.s3a.endpoint` = R2, path-style access), appends, runs `OPTIMIZE` and `VACUUM` (dry run). | delta-rs and DuckDB read Spark's versions; compaction reduces file count; no reader errors. |
| 9 | Table-feature compatibility: inspect protocol versions and features after each engine writes (deletion vectors, column mapping, timestamp types). | One property baseline that all three engines read and write. If Spark enables a feature that delta-rs or DuckDB cannot handle, record the property that disables it. |
| 10 | Cost check: count R2 operations for one simulated hourly run (12 appends plus one checkpoint), per engine, from Cloudflare's analytics. | Class A projected below 5% of the free tier per month; per-run caps in ADR-0015 confirmed or revised from these numbers. |
| 11 | Analytics latency: time from a burst of known requests to their appearance in the GraphQL analytics. | Recorded; sets how far the post-run check in ADR-0015 can be trusted. |

**Spend limit for the spike itself:** at most 20,000 Class A and 50,000 Class B operations in total (2% and 0.5% of the monthly allowance). `ge_intel.cost_guard` runs before every step and the spike stops if it blocks.

## Outputs

- `docs/spikes/S1_results.md`: versions, configuration snippets (no secrets), the property baseline, pass or fail per step, measured operation counts, and a go or no-go for ADR-0003.
- ADR-0003 updated to Accepted, or superseded (fallback: plain Parquet plus manifest).
- Version pins for ADR-0011.

## Fallbacks

- Spark fails on R2 but delta-rs passes: Spark writes only to its own tables, and delta-rs merges results into shared tables.
- delta-rs fails on R2: supersede ADR-0003 with plain Parquet plus a manifest.

## Clean-up

Delete `ge-intel-spike` and its token. Remove the throwaway virtualenv.
