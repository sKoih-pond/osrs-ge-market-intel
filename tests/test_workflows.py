"""The `history` workflow (ADR-0020): started by hand only, its own single writer, and R2
secrets only in the steps that read or write R2 (ADR-0014)."""

from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"


def load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def test_the_history_job_runs_only_when_started_by_hand():
    doc = load("history.yml")
    triggers = doc.get("on", doc.get(True))  # YAML 1.1 reads a bare `on` as true
    assert list(triggers) == ["workflow_dispatch"]  # no schedule: Sylvester starts each run
    assert doc["concurrency"] == {"group": "ge-intel-history-writer", "cancel-in-progress": False}
    assert load("hourly.yml")["concurrency"]["group"] != doc["concurrency"]["group"]


def test_secrets_reach_only_the_steps_that_use_r2():
    steps = load("history.yml")["jobs"]["run"]["steps"]
    with_secrets = [s for s in steps if "secrets." in str(s.get("env", {}))]
    assert with_secrets
    for step in with_secrets:
        assert any(f"ge-intel history {s}" in step["run"] for s in ("pull", "push", "upload"))
