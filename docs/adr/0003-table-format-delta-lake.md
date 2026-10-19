# ADR-0003: Table format: Delta Lake

- **Status:** Accepted; spike S1 passed on 2026-10-20 (`docs/spikes/S1_results.md`)

> **Confirmed by S1 (2026-10-20):** delta-rs, DuckDB, dbt-duckdb and Spark all read and write the shared tables on R2. Rules adopted: one commit per table per run; checkpoint every run; gold tables written by delta-rs with overwrite or replace-where (DuckDB `INSERT` is append-only); `vacuum(full=True)` so crashed writers' files are cleaned; protocol baseline reader 1 / writer 2 with no table features, tables created by delta-rs; `aws_conditional_put: etag` set explicitly. Pins: deltalake 1.6.6, duckdb 1.5.6, dbt-duckdb 1.11.0, pyspark 4.2.0, delta-spark 4.4.1, hadoop-aws 3.5.0, JDK 17.
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 8, option 4)

> **Amended by ADR-0015 (2026-10-20):** weekly compaction and `VACUUM` with 7-day retention keep old file versions from accumulating, so time travel reaches back 7 days. Training-set reproducibility rests on immutable partitions, the recorded table version and a row hash.

## Context

Three engines will touch the same tables: a Python hourly writer, DuckDB (and dbt-duckdb) for transforms and reads, and Spark for the backtest grid and full-history reprocessing (ADR-0011). The brief asks for reproducible historical snapshots. Plain Parquet folders need a hand-written manifest to show which files are complete, and offer no row-level edits, safe compaction or time travel.

Facts checked on 2026-10-20:
- R2 evaluates conditional `PutObject` headers (`If-None-Match`, `If-Match`) atomically.
- delta-rs (`deltalake`) documents native R2 support, using `aws_conditional_put: 'etag'` for safe concurrent writes without DynamoDB.
- DuckDB's Delta extension supports `INSERT` (including multiple inserts in one transaction as one version) and time-travel reads, but not `UPDATE`, `DELETE` or `MERGE` (DuckDB blog, May 2026).
- Spark's behaviour on R2 (S3A, Delta's S3 log store) is not yet verified.

## Decision

- Silver and gold tables are Delta tables in `ge-intel-lake` (ADR-0002).
- **Hourly writes use delta-rs** with `aws_conditional_put: 'etag'`. Idempotent re-runs replace the run's interval with a predicate overwrite (replace-where), so a repeated run commits the same data.
- **One writer per table at a time**, enforced by the orchestrator (Actions concurrency groups in Phase B, Airflow pools in Phase C). Cross-engine concurrent writes are not relied on, even if the spike shows they work.
- Spark writes only to the tables it owns (backtest results, reprocessed versions) or during scheduled maintenance (compaction), never concurrently with the hourly writer on the same table.
- Training sets record the Delta version they read, so any model can be rebuilt from exactly the same data.
- A baseline of table properties (reader and writer versions, table features such as deletion vectors) is fixed by spike S1 so all three engines can read every table.

## Consequences

- The Delta log replaces the hand-made completeness manifest.
- Compaction (OPTIMIZE) and clean-up (VACUUM) become scheduled maintenance tasks.
- If S1 fails for Spark only, Spark writes to its own tables and delta-rs merges results. If it fails for delta-rs on R2, fall back to plain Parquet with a manifest (the discovery report's original design) and supersede this ADR.

## Alternatives considered

- **Plain Parquet plus manifest:** simplest, and the fallback.
- **Apache Iceberg:** needs a catalog service; weaker Python write story at the time of writing.
- **DuckLake:** promising, younger, and less recognised by employers.

## References

- R2 S3 API compatibility: https://developers.cloudflare.com/r2/platform/s3-compatibility/api/
- delta-rs, Cloudflare R2 and MinIO: https://github.com/delta-io/delta-rs/blob/main/docs/integrations/object-storage/s3-like.md
- DuckDB, "Delta Grows Up: Writes, Unity Catalog and Time Travel": https://duckdb.org/2026/05/07/delta-uc-updates.html
