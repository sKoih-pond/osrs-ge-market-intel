"""The `history` workflow (ADR-0020): run only on request, its own single writer, responses
saved after every step that asks the Wiki, and R2 secrets only in the steps that read or write
R2 (ADR-0014)."""

from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
WIKI_STEPS = ("check-1h", "fill", "check-and-fill")


def load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def steps() -> dict[str, dict]:
    return {s.get("name", s.get("uses")): s for s in load("history.yml")["jobs"]["run"]["steps"]}


def test_the_history_job_runs_only_on_request():
    doc = load("history.yml")
    triggers = doc.get("on", doc.get(True))  # YAML 1.1 reads a bare `on` as true
    assert list(triggers) == ["workflow_dispatch"]  # no GitHub schedule (ADR-0017)
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["dry_run"]["default"] is True  # a bare request sends and writes nothing
    assert set(WIKI_STEPS) <= set(inputs["step"]["options"])
    assert doc["concurrency"] == {"group": "ge-intel-history-writer", "cancel-in-progress": False}
    assert load("hourly.yml")["concurrency"]["group"] != doc["concurrency"]["group"]


def test_every_step_that_asks_the_wiki_saves_what_it_fetched():
    asks, saves = steps()["Wiki requests"]["if"], steps()["Save new raw Wiki responses to R2"]["if"]
    assert saves.startswith("always()")  # also after a step that stopped partway
    for step in WIKI_STEPS:
        assert f'"{step}"' in asks and f'"{step}"' in saves


def test_secrets_reach_only_the_steps_that_use_r2():
    with_secrets = [s for s in steps().values() if "secrets." in str(s.get("env", {}))]
    assert with_secrets
    for step in with_secrets:
        used = ("pull", "push", "upload", "parity")
        dag = step["run"] == "orchestration/ci/run_airflow.sh"  # its tasks pull and compare
        assert dag or any(f"ge-intel history {s}" in step["run"] for s in used)


def test_reprocess_and_benchmark_only_read():
    """The PySpark rebuild and the benchmark never write to R2 or ask the Wiki (ADR-0011,
    amended): every step they run is theirs or reads, and the writing steps exclude them."""
    batch = ("reprocess", "benchmark", "airflow")
    for step in steps().values():
        cond, run = str(step.get("if", "")), step.get("run", "")
        if any(f"ge-intel history {w}" in run for w in ("push", "upload", "check-1h", "fill")):
            assert not any(b in cond for b in batch), step.get("name")
    parity = steps()["Parity with the DuckDB build and R2"]
    assert parity["if"] == "inputs.step == 'reprocess'" and "--r2" in parity["run"]
    assert "R2_ACCESS_KEY_ID" in parity["env"] and "CLOUDFLARE_ANALYTICS_TOKEN" not in parity["env"]
    doc = load("history.yml")
    assert doc["jobs"]["run"]["runs-on"] == "ubuntu-24.04"  # one image: timings compare
    assert "env" not in steps()["Install Airflow 3.3.2"]  # third-party installs see no secrets


def test_airflow_processes_never_hold_the_credentials():
    """run_airflow.sh moves them into a file only the R2-reading tasks load, and unsets them
    before it starts Postgres or any Airflow process (the DAG test checks who loads it)."""
    script = (WORKFLOWS.parents[1] / "orchestration" / "ci" / "run_airflow.sh").read_text()
    unset = script.index("unset R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY CLOUDFLARE_ANALYTICS_TOKEN")
    assert unset < script.index("systemctl start postgresql") < script.index('airflow" db migrate')
