"""History backfill and reprocessing, one month per mapped task (ADR-0004, amended).

Every task is a call to the task contract, `ge-intel run <task> --start --end`: Airflow
schedules, retries and records; the tasks hold the logic and record completion in storage, so
a second run of the DAG finds every month complete and skips it. Triggered by hand with the
month range as parameters (Airflow 3 gives a manual run no data interval).

    cost_guard -> history_download -> history_pull -> month_list
      -> month[i]: build_1h_duckdb, reprocess_1h_spark -> parity_1h     (one group per month)
      -> history_r2_check

The DuckDB and PySpark builds each write one table, so each has a pool of one slot: one writer
per table (ADR-0004, contract rule 6), while the two engines and the parity checks of other
months run side by side. Nothing writes to R2; only the pull and the final check read it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, get_current_context, task, task_group

DAG_ID = "ge_history_reprocess"
FIRST_MONTH, END_MONTH = "2021-03-01", "2026-11-01"  # the dump's 68 months
POOLS = {"duckdb": "history_duckdb", "spark": "history_spark"}
RUN = '"$GE_INTEL" run {task} --start {start} --end {end}'
SPAN = "{{ params.start }}T00:00:00Z", "{{ params.end }}T00:00:00Z"
RETRIES = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=5),
}


def months(start: str, end: str) -> list[dict[str, str]]:
    """Whole UTC months from `start` up to `end` (YYYY-MM-01 dates), as task intervals."""
    first = datetime.fromisoformat(start).replace(tzinfo=UTC)
    last = datetime.fromisoformat(end).replace(tzinfo=UTC)
    if first.day != 1 or last.day != 1 or first >= last:
        raise ValueError(f"need two month starts in order, got {start} and {end}")
    out, cur = [], first
    while cur < last:
        nxt = datetime(cur.year + cur.month // 12, cur.month % 12 + 1, 1, tzinfo=UTC)
        iso = "%Y-%m-%dT%H:%M:%SZ"
        out.append(
            {"start": cur.strftime(iso), "end": nxt.strftime(iso), "label": cur.strftime("%Y-%m")}
        )
        cur = nxt
    return out


def contract(task_name: str, start: str, end: str) -> str:
    return RUN.format(task=task_name, start=start, end=end)


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=RETRIES,
    params={
        "start": Param(FIRST_MONTH, type="string", pattern=r"^\d{4}-\d{2}-01$"),
        "end": Param(END_MONTH, type="string", pattern=r"^\d{4}-\d{2}-01$"),
    },
    tags=["history", "backfill"],
    doc_md=__doc__,
)
def ge_history_reprocess():
    # One month-to-date check of R2 usage before anything reads it; fails closed, no retry.
    guard = BashOperator(task_id="cost_guard", bash_command='"$GE_INTEL" guard', retries=0)
    download = BashOperator(
        task_id="history_download", bash_command=contract("history_download", *SPAN)
    )
    pull = BashOperator(task_id="history_pull", bash_command=contract("history_pull", *SPAN))

    @task
    def month_list() -> list[dict[str, str]]:
        params = get_current_context()["params"]
        return months(params["start"], params["end"])

    def labelled(label: str) -> None:
        get_current_context()["month"] = label  # names the mapped instance in the grid

    @task.bash(pool=POOLS["duckdb"], map_index_template="{{ month }}")
    def build_1h_duckdb(start: str, end: str, label: str) -> str:
        labelled(label)
        return contract("history_build_1h", start, end)

    @task.bash(pool=POOLS["spark"], map_index_template="{{ month }}")
    def reprocess_1h_spark(start: str, end: str, label: str) -> str:
        labelled(label)
        return contract("reprocess_1h", start, end)

    @task.bash(map_index_template="{{ month }}")
    def parity_1h(start: str, end: str, label: str) -> str:
        labelled(label)
        return contract("parity_1h", start, end)

    @task_group(group_id="month")
    def month(start: str, end: str, label: str):
        built = build_1h_duckdb(start, end, label)
        rebuilt = reprocess_1h_spark(start, end, label)
        [built, rebuilt] >> parity_1h(start, end, label)

    r2_check = BashOperator(
        task_id="history_r2_check", bash_command=contract("history_r2_check", *SPAN)
    )

    listed = month_list()
    guard >> download >> pull >> listed
    month.expand_kwargs(listed) >> r2_check


ge_history_reprocess()
