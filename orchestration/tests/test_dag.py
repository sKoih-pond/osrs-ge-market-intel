"""The history DAG imports cleanly and keeps its shape (ADR-0004, amended): every task calls the
contract, months are mapped as one group each, each engine's writer has a pool of one, and
every task but the cost guard retries. Runs in Airflow's own environment (CI job `airflow`)."""

import sys
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))
import ge_history_reprocess as module  # noqa: E402


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), known_pools={"default_pool", *module.POOLS.values()})
    assert bag.import_errors == {}
    return bag.dags[module.DAG_ID]  # parsed only: no metadata database needed


def test_the_dag_runs_only_when_triggered(dag):
    assert dag.schedule is None and not dag.catchup and dag.max_active_runs == 1
    assert dag.params["start"] == module.FIRST_MONTH and dag.params["end"] == module.END_MONTH


def test_months_are_mapped_one_group_each(dag):
    assert set(dag.task_ids) == {
        "cost_guard",
        "history_download",
        "history_pull",
        "month_list",
        "month.build_1h_duckdb",
        "month.reprocess_1h_spark",
        "month.parity_1h",
        "history_r2_check",
    }
    group = dag.task_group_dict["month"]
    assert type(group).__name__ == "MappedTaskGroup"
    parity = dag.get_task("month.parity_1h")
    both = {"month.build_1h_duckdb", "month.reprocess_1h_spark"}
    assert parity.upstream_task_ids == both | {"month_list"}  # mapped from the month list
    assert dag.get_task("history_r2_check").upstream_task_ids == {"month.parity_1h"}


def test_one_writer_per_table_and_retries(dag):
    assert dag.get_task("month.build_1h_duckdb").pool == "history_duckdb"
    assert dag.get_task("month.reprocess_1h_spark").pool == "history_spark"
    for t in dag.tasks:
        assert t.retries == (0 if t.task_id == "cost_guard" else 2), t.task_id


def test_every_command_is_the_contract():
    assert module.contract("parity_1h", "a", "b") == '"$GE_INTEL" run parity_1h --start a --end b'


def test_the_full_range_is_the_dumps_68_months():
    found = module.months(module.FIRST_MONTH, module.END_MONTH)
    assert len(found) == 68
    assert found[0] == {
        "start": "2021-03-01T00:00:00Z",
        "end": "2021-04-01T00:00:00Z",
        "label": "2021-03",
    }
    assert found[-1]["label"] == "2026-10" and found[-1]["end"] == "2026-11-01T00:00:00Z"
    assert [m["label"] for m in module.months("2026-12-01", "2027-02-01")] == ["2026-12", "2027-01"]
    with pytest.raises(ValueError):
        module.months("2026-01-02", "2026-02-01")


def test_only_the_tasks_that_read_r2_load_its_credentials(dag):
    loads = {t.task_id for t in dag.tasks if "GE_SECRETS" in str(getattr(t, "bash_command", ""))}
    assert loads == {"cost_guard", "history_pull", "history_r2_check"}
