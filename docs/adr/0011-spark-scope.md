# ADR-0011: Spark scope: backtest grid and reprocessing only

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 8)

> **Amended by ADR-0015 (2026-10-20):** Spark jobs copy the partitions they need from R2 to local disk once, compute locally, and upload results as a few large files, so Spark's many small `HEAD` and `LIST` calls never reach R2. Each job runs under its own operation budget and timeout.

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
