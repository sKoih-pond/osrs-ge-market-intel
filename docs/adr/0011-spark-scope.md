# ADR-0011: Spark scope: backtest grid and reprocessing only

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 8)

> **Spike S1 (2026-10-20):** Spark 4.2.0 with delta-spark 4.4.1 and hadoop-aws 3.5.0 reads, appends to, compacts and vacuums the delta-rs tables on R2 over S3A without changing the protocol. Requires JDK 17 and `PYSPARK_PYTHON` set to the project interpreter. A 25-row append with compaction took 3 minutes 19 seconds and about 66 Class A and 872 Class B operations, which confirms the local-copy rule below and keeping Spark out of the hourly path.

> **Amended by ADR-0015 (2026-10-20):** Spark jobs copy the partitions they need from R2 to local disk once, compute locally, and upload results as a few large files, so Spark's many small `HEAD` and `LIST` calls never reach R2. Each job runs under its own operation budget and timeout.

> **Amended 2026-11-05: reprocessing first.** The reprocessing job (decision 2) comes forward from M7 and the backtest grid (decision 1) stays in M4. Its first table is `history/prices_1h` (68 months from M2): the largest table, and the one whose build carries the most rules (the dump's mixed column widths, the stored fills and the seam, the 2026-01 repair from `/1h`). An independent PySpark build (`engines/src/ge_engines/spark_job.py`) reads the same inputs on a hand-started runner and is compared month by month with the DuckDB build and with R2 (`src/ge_intel/parity.py`, `ge-intel history parity`).
>
> - **Tolerance for this table**, in place of "1e-9 relative for floats": counts, keys, volumes, regime and source exact; prices exact, and a price one rounding step (0.01 GP) away is counted as a rounding flip and reported, while anything larger fails the job. Prices are held to 0.01 GP, so a relative float tolerance does not describe them. Per-month checksums, computed apart from the row comparison, back it up: a month whose rows all compare equal but whose checksums differ fails too.
> - **Rounding is written to agree.** Spark's `round(x, 2)` rounds the shortest decimal form half up (1.005, a double just below it, becomes 1.01 where DuckDB gives 1.0), so the Spark job rounds `x * 100` to a whole number and divides by 100, as DuckDB does. In Polars, dividing by a literal compiles to multiplying by its reciprocal and moves the last bit of about one price in seven, so the benchmark uses Polars' own `round(x, 2)`. Tests check each against DuckDB on 20,000 to 40,000 random prices.
> - **Its own project.** The engines live in `engines/` with their own lockfile: the hourly path's `uv sync --locked` never installs PySpark or Polars, and delta-spark's pins (pyspark 4.2.0 at most) never meet dbt's.
> - **Never on R2.** Spark reads only local disk (the ADR-0015 amendment above): the runner restores the inputs once (`ge-intel history pull`), and parity reads R2's Delta log and manifest and copies its data files, one GET each. Nothing is written to R2.
> - **The benchmark** times the same job (the 68 months and the fills to hourly Parquet, the one-month repair left out) in DuckDB, Polars and PySpark, in fresh processes, three runs each, with the same threads, memory cap and spill disk, and counts a timing only when that engine's output matches the others month by month.

## Context

Data volume is moderate: about 145M hourly rows of history and about 200M 5-minute rows added per year. DuckDB processes this on one machine and runs the hourly path in seconds. Two workloads have a different shape from the hourly path:

1. **The backtest and sensitivity grid (M4).** Thousands of independent train-and-evaluate tasks: 3 policies (ADR-0008) x about 12 fill-assumption settings x about 250 weekly test windows, roughly 9,000 tasks. Each is CPU-bound Python (fill simulation, LightGBM). A full grid runs for hours, and one failed task should not force a restart of the whole grid.
2. **Full-history reprocessing (M7).** When a feature or policy definition changes version, gold tables must be rebuilt over all history. The hourly incremental path and a full rebuild can drift apart silently, and that drift is a correctness risk.

## Decision

PySpark is used for exactly these two jobs.

1. **Backtest grid.** Work is grouped by (policy, setting, week) and executed with `applyInPandas`. Spark schedules the groups across available cores, retries failed tasks, and re-runs only what was lost. The same code runs on a 4-core Actions runner, the Phase C VM, or more machines without changes. Results go to Spark-owned Delta tables.
2. **Reprocessing with a parity check.** Spark rebuilds the affected gold tables using window functions partitioned by item. For overlapping periods, its output must match the incremental DuckDB path within stated tolerances (exact for counts, 1e-9 relative for floats); any difference fails the job. Two independent implementations of the same logic, compared automatically, catch bugs in either one.

A benchmark of DuckDB, Polars and PySpark on these two jobs (runtime, peak memory, code size) is written up with the reprocessing work, so the choice of engine per job is backed by measurements.

Not used for:
- **The hourly pipeline:** Spark's startup and coordination overhead exceeds the work in an hourly increment.
- **The one-off conversion of raw backfill files:** about 3 GB, done once; DuckDB is simpler.
- **Streaming:** the source publishes closed 5-minute aggregates; there is no event stream.
- **Model training via MLlib:** LightGBM is the model.

**Where it runs**
- Development: small samples on the laptop. Spark 4.x needs Java 17; the laptop has Java 8, so a JDK 17 install (Sylvester's approval) or a container is required.
- Phase B: hand-started GitHub Actions workflows (16 GB, 4 vCPU) for full runs.
- Phase C: triggered by Airflow on the VM.

## Consequences

- On one machine Spark may be slower than DuckDB for the reprocessing job. The benchmark reports that plainly; the justification is the independent parity check and the path to scale out, not single-machine speed.
- Feature logic exists twice (DuckDB SQL for the incremental path, Spark for rebuilds). This is limited to gold-table features, and the parity check keeps the two in step.
- Version pins (Spark, delta-spark, hadoop-aws) are set by spike S1.

## Alternatives considered

- **joblib or multiprocessing for the grid:** parallel on one machine, but no task-level retry or path to more machines.
- **Ray or Dask:** capable, but add a runtime that the rest of the stack (Delta, Airflow providers) supports less directly.
- **DuckDB only:** simplest, but gives up the independent parity check and the scale-out path.
- **Spark for the raw-file conversion:** the volume does not justify it.

## References

- Kickoff decisions log, decision 8; discovery report section 6.6.
