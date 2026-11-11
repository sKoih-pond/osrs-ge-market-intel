# ADR-0004: Orchestration: Airflow-ready tasks now, Airflow 3 at Phase C

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 3)

> **Implemented in M1 (ADR-0016, 2026-10-20):** `ge_intel.tasks.REGISTRY` is the single task list; `ge-intel run <task> --start --end` is the interface and `ge-intel hourly` the Phase B scheduler shim. Completion is recorded in `ops/task_runs` (one commit per process) and, for `/5m`, in the bucket manifest. Two schedule slots an hour; dbt runs in its own process.

> **Amended 2026-11-05: batch and backfill DAGs brought forward.** The history rebuild and its reprocessing (M2, ADR-0011 as amended) run under Airflow 3 now, as the first DAG: `ge_history_reprocess` (`orchestration/dags/`), one mapped task group per month, every task a call to the contract except the cost guard (`ge-intel guard`) and the step that lists the months. The Phase C move is unchanged: the hourly path stays on GitHub Actions with ge-clock (ADR-0017), and Airflow on the VM remains the plan for it.
>
> - **Why now:** the history work is the backfill case this decision chose Airflow for: interval-shaped, partitioned by month, long (68 months, two engines and a parity check per month), with per-partition retries. Running it now proves the contract carries over before the VM exists, rather than at migration time.
> - **On-request tasks:** `ge_intel.history_tasks.REGISTRY` (`history_download`, `history_pull`, `history_build_1h`, `reprocess_1h`, `parity_1h`, `history_r2_check`) takes whole-month intervals, and the tasks that build or compare take exactly one month. It is a separate list from `tasks.REGISTRY`, so the hourly run never executes them. Their run records go to a local lake (`GE_LAKE_ROOT`): the CLI refuses R2 for them, so a backfill is never a second writer of the live `ops/task_runs`.
> - **Where it runs:** a hand-started GitHub Actions job (`history` workflow, step `airflow`). Airflow 3.3.2 runs in its own environment (its constraints pin pyarrow and importlib-metadata differently from the project's lock), with the runner's preinstalled Postgres and the LocalExecutor. The job triggers the DAG through the REST API, then triggers it again to show every month skipped as complete. The runner is thrown away; the evidence is the exported runs and task instances, the grid view and the run records in storage.
> - **One writer per table** (contract rule 6) holds through an Airflow pool of one slot for each engine's build; the parity checks of other months run alongside.
> - **A manual run carries its range as parameters.** Airflow 3 gives a manually triggered run no data interval, so the DAG takes `start` and `end` (month starts) and maps over the months between them. Interval-driven scheduling (`CronDataIntervalTimetable`) stays the plan for the hourly path at Phase C.

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
