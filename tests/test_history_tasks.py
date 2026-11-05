"""History and reprocessing on the task contract (ADR-0004, amended): whole-month intervals,
the same rows as the history job's own build, completion recorded in local storage (so a
re-run skips), and never a writer of the live lake."""

import dataclasses
import json
import shutil
from datetime import UTC, datetime

import pytest
from conftest import FIXTURE_NOW, Clock, FixtureTransport
from test_history import KAGGLE, LIVE, OUTAGE, fixture_pins, hashes, local_lake

from ge_intel import cli, history, history_tasks, kaggle, ops, tasks, wiki
from ge_intel.lake import Lake

OCT = ("--start", "2026-10-01T00:00:00Z", "--end", "2026-11-01T00:00:00Z")


@pytest.fixture
def hist(tmp_path):
    """The fixture month as the history job holds it (as `test_history.h`)."""
    root = tmp_path / "history"
    shutil.copytree(KAGGLE, root / "kaggle" / kaggle.DATASET / "v1")
    client = wiki.WikiClient(
        transport=FixtureTransport(drop_buckets=[OUTAGE]),
        clock=Clock(FIXTURE_NOW),
        min_interval_s=0,
    )
    return history.History(root, client=client, live_start=LIVE, pins=fixture_pins())


@pytest.fixture
def env(hist, tmp_path, monkeypatch):
    """A history directory with every input on disk (dump, fills, checks), as after `pull`."""
    history.build(hist)
    history.check_1h(hist)
    history.fill(hist)
    monkeypatch.setenv("GE_HISTORY_DIR", str(hist.root))
    monkeypatch.setenv("GE_LAKE_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("GE_ORCHESTRATOR", "airflow")
    monkeypatch.setattr(
        history_tasks,
        "local_history",
        lambda: history.History(hist.root, live_start=LIVE, pins=fixture_pins()),
    )
    return hist


def run(task, *interval):
    return cli.main(["run", task, *(interval or OCT)])


def test_intervals_are_whole_months():
    d = lambda *a: datetime(*a, tzinfo=UTC)  # noqa: E731
    assert history_tasks.months_in(d(2021, 3, 1), d(2021, 6, 1)) == [
        "2021-03",
        "2021-04",
        "2021-05",
    ]
    assert history_tasks.months_in(d(2026, 12, 1), d(2027, 1, 1)) == ["2026-12"]
    with pytest.raises(ValueError, match="start of a UTC month"):
        history_tasks.months_in(d(2026, 1, 2), d(2026, 2, 1))
    with pytest.raises(ValueError, match="one month at a time"):
        history_tasks.one_month(d(2026, 1, 1), d(2026, 3, 1))


def test_history_tasks_never_run_in_the_hourly_list():
    assert not set(history_tasks.REGISTRY) & set(tasks.REGISTRY)
    assert {t.cadence for t in history_tasks.REGISTRY.values()} == {"on-request"}


def test_a_month_built_alone_equals_the_full_build(env, tmp_path):
    full = hashes(env.lake)[history.PRICES_1H]
    shutil.rmtree(env.root / "lake")
    assert run("history_build_1h") == 0
    assert hashes(env.lake)[history.PRICES_1H] == full

    runs = local_lake(tmp_path / "runs").read(ops.TASK_RUNS).to_pylist()
    assert [(r["task"], r["status"], r["orchestrator"]) for r in runs] == [
        ("history_build_1h", "success", "airflow")
    ]
    assert run("history_build_1h") == 0  # recorded complete in storage: skipped
    assert local_lake(tmp_path / "runs").read(ops.TASK_RUNS).num_rows == 1


def test_a_repaired_month_built_alone_takes_the_same_hours(hist, monkeypatch):
    """As `test_history`'s repair test: the fixture month made hourly-only, one `/1h` hour
    tampered; built alone, the month replaces the same hours and writes the same rows."""
    monkeypatch.setattr(history, "REPAIR_FROM_1H", frozenset({"2026-10"}))
    h = dataclasses.replace(hist, window_5m_start=LIVE)
    history.build(h)
    history.fill(h)
    tampered = datetime(2026, 10, 19, 5, tzinfo=UTC)
    payload = h.client.transport.hours[str(int(tampered.timestamp()))]
    del payload["data"][min(payload["data"], key=int)]
    full = history.check_1h(h)["build"]["repaired"]["2026-10"]["replaced"]
    expected = hashes(h.lake)[history.PRICES_1H]
    shutil.rmtree(h.root / "lake")
    alone = history.build_hourly(h, "2026-10")
    assert alone["replaced"] == full == [history.iso(tampered), history.iso(LIVE.replace(hour=11))]
    assert hashes(h.lake)[history.PRICES_1H] == expected


def test_history_tasks_refuse_the_live_lake(env, monkeypatch, capsys):
    monkeypatch.delenv("GE_LAKE_ROOT")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "x")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "x")
    assert run("history_build_1h") == 2
    assert "local directory" in capsys.readouterr().err


def test_parity_passes_on_equal_builds_and_fails_loudly_otherwise(env, tmp_path, monkeypatch):
    other = tmp_path / "other"
    shutil.copytree(env.root / "lake", other)
    monkeypatch.setenv("GE_SPARK_LAKE", str(other))
    assert run("parity_1h") == 0
    rows = local_lake(tmp_path / "runs").read(ops.TASK_RUNS).to_pylist()
    assert json.loads(rows[-1]["detail"])["exact"] is True

    tampered = Lake(root=str(other), store=None).read(history.PRICES_1H).to_pylist()
    tampered[0]["high_volume"] += 1
    local_lake(other).write(
        history.PRICES_1H,
        history.conform(
            history.pa.Table.from_pylist(tampered, schema=history.PRICES_1H_SCHEMA),
            history.PRICES_1H_SCHEMA,
        ),
        partition_by=["bucket_month"],
    )
    assert run("parity_1h", "--force", *OCT) == 1


def test_r2_check_and_pull_read_the_published_history(env, tmp_path, monkeypatch):
    published = local_lake(tmp_path / "r2")  # R2 stands in as a local lake
    history.upload(env, published, stored_bytes=0)
    history.push_raw(env, published)
    monkeypatch.setenv("GE_HISTORY_SOURCE", str(tmp_path / "r2"))
    assert run("history_r2_check") == 0

    fresh = tmp_path / "fresh"
    monkeypatch.setattr(
        history_tasks,
        "local_history",
        lambda: history.History(fresh, live_start=LIVE, pins=fixture_pins()),
    )
    late = ("--start", "2026-10-01T00:00:00Z", "--end", "2026-11-01T00:00:00Z")
    assert run("history_pull", *late) == 0
    assert len(history.raw_files(history.History(fresh, pins=[]))) == len(history.raw_files(env))
    early = ("--start", "2026-09-01T00:00:00Z", "--end", "2026-10-01T00:00:00Z")
    shutil.rmtree(fresh)
    assert run("history_pull", *early) == 0
    assert not (fresh / "bronze").exists()  # nothing stored before October


def test_a_repaired_month_refuses_to_build_without_every_1h_answer(hist, monkeypatch):
    """`build` leaves such a month unrepaired and unvalidated; a task, which runs after the
    pull, refuses instead of writing it."""
    monkeypatch.setattr(history, "REPAIR_FROM_1H", frozenset({"2026-10"}))
    h = dataclasses.replace(hist, window_5m_start=LIVE)
    history.build(h)
    with pytest.raises(history.NotReady, match="/1h answers not on disk"):
        history.build_hourly(h, "2026-10")


def test_a_task_ended_hard_still_records_its_run(tmp_path):
    """Under Airflow a history task ends with os._exit once its record is committed
    (GE_EXIT_HARD, see cli.py); the record and the exit code must survive that."""
    import os
    import subprocess
    import sys

    env = {**os.environ, "GE_LAKE_ROOT": str(tmp_path / "runs"), "GE_EXIT_HARD": "1"}
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "ge_intel.cli",
            "run",
            "history_build_1h",
            "--start",
            "2026-01-02T00:00:00Z",
            "--end",
            "2026-02-01T00:00:00Z",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert done.returncode == 1 and "start of a UTC month" in done.stderr
    runs = local_lake(tmp_path / "runs").read(ops.TASK_RUNS).to_pylist()
    assert [(r["task"], r["status"]) for r in runs] == [("history_build_1h", "failed")]
