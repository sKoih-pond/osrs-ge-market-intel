"""Forced rebuilds of old hours (ADR-0019 amendment of 12 November 2026), on recorded fixtures.

The fixture hour ends at 22:00 UTC on 19 October 2026, so it is among the first days the weekly
run of Monday 23 November deletes from: rule A1 drops the hour's rows that are not ideas and
rule B1 its /latest day. A forced rebuild of the hour after that would regrade it and overwrite
its idea rows, so it is refused. The newest closed hour still builds and publishes as before,
forced or not, and never reaches the guard.
"""

import dataclasses
import json
import shutil
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from conftest import FIXTURE_END, FIXTURE_NOW, make_context

from ge_intel import ingest, maintain, tasks
from ge_intel.build import SNAPSHOT, RebuildRefused, hour_scope
from ge_intel.lake import content_hash
from ge_intel.maintain import PROPOSED
from ge_intel.ops import TASK_RUNS
from ge_intel.policies import CASUAL_V1

HOUR_START = FIXTURE_END - timedelta(hours=1)
WEEKLY_END = datetime(2026, 11, 23, tzinfo=UTC)  # the first weekly run that deletes rows
AFTER_WEEKLY = WEEKLY_END + timedelta(minutes=10)
NEXT_DAY = FIXTURE_NOW + timedelta(days=1)  # the fixture hour is old, its /latest still kept
KEYS = ["snapshot_at", "item_id"]


def never(ctx, start, end):
    raise AssertionError("the guard ran for the newest closed hour")


def without_guard(mp) -> None:
    """Replace the guard with one that fails the test if it is ever called."""
    task = dataclasses.replace(tasks.REGISTRY["build_snapshot"], refuse_old=never)
    mp.setitem(tasks.REGISTRY, "build_snapshot", task)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """One scheduled run for the fixture hour, as the hourly workflow makes it."""
    root = tmp_path_factory.mktemp("guard")
    mp = pytest.MonkeyPatch()
    mp.setenv("GE_DIST_DIR", str(root / "dist"))
    mp.delenv("GE_DEPLOY", raising=False)
    without_guard(mp)
    ctx = make_context(root / "lake")
    code = tasks.hourly(ctx, FIXTURE_END)
    mp.undo()
    return {"code": code, "root": root, "ctx": ctx}


@pytest.fixture
def lake(built, tmp_path, monkeypatch):
    """A copy of the built lake, so each test changes only its own."""
    monkeypatch.setenv("GE_DIST_DIR", str(tmp_path / "dist"))
    monkeypatch.delenv("GE_DEPLOY", raising=False)
    shutil.copytree(built["root"] / "lake", tmp_path / "lake")
    return tmp_path / "lake"


def weekly_retention(root):
    """The weekly run of 23 November: rules A1 and B1 reach 19 October."""
    ctx = make_context(root, now=AFTER_WEEKLY)
    out = maintain.apply_retention(ctx, PROPOSED, WEEKLY_END - timedelta(days=7), WEEKLY_END)
    assert out[ingest.LATEST]["rows_deleted"] > 0 and out[SNAPSHOT]["rows_deleted"] > 0
    return ctx


def ideas(ctx) -> int:
    return ctx.lake.read(
        SNAPSHOT, filters=[*hour_scope(FIXTURE_END)[1], ("is_idea", "=", True)]
    ).num_rows


def refused(ctx) -> str:
    """Force a rebuild of the fixture hour; expect the refusal, recorded, and gold unchanged."""
    version = ctx.lake.table(SNAPSHOT).version()
    before = content_hash(ctx.lake.read(SNAPSHOT), KEYS)
    rows: list[dict] = []
    with pytest.raises(RebuildRefused) as caught:
        tasks.run_task(ctx, "build_snapshot", HOUR_START, FIXTURE_END, force=True, rows=rows)
    assert ctx.lake.table(SNAPSHOT).version() == version
    assert content_hash(ctx.lake.read(SNAPSHOT), KEYS) == before
    assert rows[-1]["status"] == "failed"  # in ops/task_runs, with the reason
    assert rows[-1]["detail"]["error"].startswith("RebuildRefused: refused")
    return str(caught.value)


def test_the_scheduled_run_builds_and_publishes_the_newest_hour(built):
    assert built["code"] == 0
    rows = built["ctx"].lake.read(TASK_RUNS).to_pylist()
    status = {r["task"]: r["status"] for r in rows}
    assert status["build_snapshot"] == "success" and status["publish"] == "built"
    snapshot = json.loads((built["root"] / "dist/ge/data/v1/snapshot.json").read_text())
    assert snapshot["data_as_of"] == "2026-10-19T22:00:00Z" and snapshot["ideas"]
    assert ideas(built["ctx"]) > 0


def test_a_forced_run_of_the_newest_hour_rebuilds_it_as_before(lake, monkeypatch):
    without_guard(monkeypatch)
    ctx = make_context(lake)  # 22:30 on 19 October: the fixture hour is the newest closed hour
    before = content_hash(ctx.lake.read(SNAPSHOT), KEYS)
    assert tasks.hourly(ctx, FIXTURE_END, force=True) == 0
    assert content_hash(ctx.lake.read(SNAPSHOT), KEYS) == before


def test_a_rebuild_never_replaces_another_policys_rows(lake):
    ctx = make_context(lake)
    other = ctx.lake.read(SNAPSHOT).slice(0, 3)
    other = other.set_column(
        other.schema.get_field_index("policy_id"),
        "policy_id",
        pa.array(["other-v1"] * other.num_rows, pa.string()),
    )
    ctx.lake.write(SNAPSHOT, other, mode="append")
    casual = [("policy_id", "=", CASUAL_V1.id)]
    before = content_hash(ctx.lake.read(SNAPSHOT, filters=casual), KEYS)
    rows: list[dict] = []
    tasks.run_task(ctx, "build_snapshot", HOUR_START, FIXTURE_END, force=True, rows=rows)
    assert content_hash(ctx.lake.read(SNAPSHOT, filters=casual), KEYS) == before
    kept = ctx.lake.read(SNAPSHOT, filters=[("policy_id", "=", "other-v1")])
    assert content_hash(kept, KEYS) == content_hash(other, KEYS)


def test_after_the_weekly_retention_a_forced_rebuild_is_refused(lake):
    ctx = weekly_retention(lake)
    kept = ideas(ctx)
    message = refused(ctx)
    assert "its /latest rows are gone" in message
    assert f"it holds {kept} idea rows" in message


def test_a_dispatch_with_an_old_end_and_force_fails_before_publishing(lake, capsys):
    """The hourly workflow's dispatch inputs `end` and `force`."""
    ctx = weekly_retention(lake)
    before = content_hash(ctx.lake.read(SNAPSHOT), KEYS)
    assert tasks.hourly(ctx, FIXTURE_END, force=True) == 1
    assert content_hash(ctx.lake.read(SNAPSHOT), KEYS) == before
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert events[-1]["outcome"] == "failed" and events[-1]["failed_task"] == "build_snapshot"
    assert "publish" not in {e["task"] for e in events if e["event"] == "task_started"}
    # The refusal's failed row does not undo the hour's success, so the same run without
    # `force` skips the hour instead of rebuilding it unchecked.
    ctx.cache.clear()
    version = ctx.lake.table(SNAPSHOT).version()
    rows: list[dict] = []
    skipped = tasks.run_task(ctx, "build_snapshot", HOUR_START, FIXTURE_END, force=False, rows=rows)
    assert skipped == {"status": "complete"}
    assert ctx.lake.table(SNAPSHOT).version() == version


def test_an_old_hour_holding_its_ideas_is_refused_with_its_latest_rows_kept(lake):
    message = refused(make_context(lake, now=NEXT_DAY))
    assert "idea rows" in message and "/latest" not in message


def test_an_old_hour_without_its_latest_rows_is_refused_with_no_rows_kept(lake):
    ctx = make_context(lake, now=AFTER_WEEKLY)
    ctx.lake.delete(ingest.LATEST, f"snapshot_date = '{FIXTURE_END.date()}'")
    ctx.lake.delete(SNAPSHOT, hour_scope(FIXTURE_END)[0])
    message = refused(ctx)
    assert "its /latest rows are gone" in message and "idea rows" not in message


def test_an_old_hour_with_its_latest_rows_and_no_rows_kept_still_builds(lake):
    ctx = make_context(lake, now=NEXT_DAY)
    first = content_hash(ctx.lake.read(SNAPSHOT), KEYS)
    ctx.lake.delete(SNAPSHOT, hour_scope(FIXTURE_END)[0])
    rows: list[dict] = []
    out = tasks.run_task(ctx, "build_snapshot", HOUR_START, FIXTURE_END, force=True, rows=rows)
    assert out["status"] == "success" and out["ideas"] > 0
    assert content_hash(ctx.lake.read(SNAPSHOT), KEYS) == first
