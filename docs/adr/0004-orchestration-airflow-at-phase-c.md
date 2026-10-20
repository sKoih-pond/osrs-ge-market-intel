# ADR-0004: Orchestration: Airflow-ready tasks now, Airflow 3 at Phase C

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 3)

> **Implemented in M1 (ADR-0016, 2026-10-20):** `ge_intel.tasks.REGISTRY` is the single task list; `ge-intel run <task> --start --end` is the interface and `ge-intel hourly` the Phase B scheduler shim. Completion is recorded in `ops/task_runs` (one commit per process) and, for `/5m`, in the bucket manifest. Two schedule slots an hour; dbt runs in its own process.

## Context

The work is interval-shaped: hourly ingestion, daily item snapshots, weekly maintenance, about 49k hourly intervals of history to backfill, dbt models, and Spark jobs (ADR-0011), with dependencies between them. It needs interval-based scheduling, backfills, retries, dependency ordering, concurrency limits per table, run history and failure alerts.

The runtime has two phases (ADR-0001). Phase B runs in short-lived GitHub Actions jobs with no persistent host. Phase C has an always-on VM. The development laptop has 8 GB of RAM.

## Decision

- **Phase B:** no orchestration framework. GitHub Actions workflows trigger task commands that follow the contract below. Because every task is idempotent and records completion in storage, the scheduler only needs to trigger; catch-up and gap-filling come from storage, not from scheduler state.
- **Phase C (M7):** Airflow 3 with Postgres on the VM. DAGs call the same task commands; dbt runs through Astronomer Cosmos; Spark jobs are triggered from Airflow; history is re-run with Airflow's backfill.
- **Migration proof:** partitions built under Airflow must match those built under Actions, compared by content hash over an overlap window.

Why Airflow at Phase C:
- **Interval semantics match the task contract.** Airflow's `data_interval_start` and `data_interval_end` map one-to-one onto the contract's `[start, end)` interval, and backfills over date ranges are built in.
- **Integrations this project needs are maintained:** Cosmos renders dbt models as tasks with their lineage; providers exist for Spark submission, Docker and Postgres; Airflow 3 adds asset-aware scheduling for data-driven triggers.
- **Easy to review and hand over.** It is the most widely used open-source orchestrator, so its DAGs, pools and alerting are familiar to most data engineers.
- **Its hosting needs match Phase C.** Airflow needs persistent services (scheduler, API server, DAG processor, metadata database). The VM provides them; running them on Actions or the laptop would not make sense.

### Task contract

1. **Interface:** `ge-intel run <task> --start <UTC ISO-8601> --end <UTC ISO-8601>`, a half-open interval `[start, end)`.
2. **Idempotent:** running a task twice for the same interval leaves the same committed state (same content hashes).
3. **State lives in storage:** completion is recorded as Delta commit metadata and an `ops.task_runs` row, never only in orchestrator memory. Catch-up asks storage what is missing.
4. **No look-ahead:** a task reads only buckets that closed before `end`.
5. **Declared dependencies:** a single task registry (task name to upstream tasks) drives both the Actions workflow and, later, the Airflow DAGs.
6. **One writer per table:** enforced by Actions concurrency groups now and Airflow pools later.
7. **Observable:** structured JSON logs; exit code 0 only on a committed success; row counts and durations written to `ops`.

## Consequences

- No orchestrator UI until after v1. Until then, the evidence of correct orchestration is the task contract, catch-up behaviour, the Actions run history and the ops tables.
- The move to Airflow is verifiable: same outputs, new platform, proven by hashes.
- Local development uses the CLI directly; running Airflow on the laptop (`airflow standalone`) is optional.

## Alternatives considered

- **Dagster now:** the closest fit for partitioned assets, and it could run headless in Actions today. Not chosen: Airflow's broader integration ecosystem and wider familiarity make the DAGs easier to review and hand over, and its interval model plus the storage-recorded completeness covers the partition tracking Dagster would add.
- **Airflow from day one on a VM:** brings the VM's account, reclamation and patching risks forward before any product exists.
- **Prefect Cloud free tier:** a hosted UI now, but limited to 5 deployments and 7 days of run history, and another external service.

## References

- Apache Airflow documentation: https://airflow.apache.org/docs/
- Astronomer Cosmos (dbt in Airflow): https://github.com/astronomer/astronomer-cosmos
- Discovery report section 6.6.
