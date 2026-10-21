"""End to end on recorded fixtures: every task, dbt included, on a local Delta lake.

Runs the scheduler once for the fixture hour, then checks what M1's acceptance criteria and
the task contract (ADR-0004) promise: no duplicate (item, bucket) rows, an idempotent re-run,
a valid v1 contract, tables on the S1 protocol baseline.
"""

import copy
import json
from datetime import timedelta

import pytest
from conftest import FIXTURE_END, make_context

from ge_intel import ingest, maintain, publish, tasks
from ge_intel.build import SNAPSHOT
from ge_intel.lake import content_hash
from ge_intel.ops import TASK_RUNS


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    root = tmp_path_factory.mktemp("e2e")
    dist = root / "dist"
    mp = pytest.MonkeyPatch()
    mp.setenv("GE_DIST_DIR", str(dist))
    mp.delenv("GE_DEPLOY", raising=False)
    ctx = make_context(root / "lake")
    code = tasks.hourly(ctx, FIXTURE_END)
    yield {"ctx": ctx, "code": code, "root": root / "lake", "dist": dist}
    mp.undo()


def test_hourly_run_succeeds(run):
    assert run["code"] == 0
    rows = run["ctx"].lake.read(TASK_RUNS).to_pylist()
    status = {r["task"]: r["status"] for r in rows}
    assert status == {
        "ingest_mapping": "success",
        "ingest_5m": "success",
        "ingest_latest": "success",
        "build_snapshot": "success",
        "publish": "built",  # no deploy outside the scheduled workflow
        "check_duplicates": "success",
        "maintain": "success",
        "hourly": "success",
    }


def test_every_fixture_row_landed_once(run):
    prices = run["ctx"].lake.read(ingest.PRICES)
    expected = sum(len(p["data"]) for p in run["ctx"].client.transport.buckets.values())
    assert prices.num_rows == expected
    assert maintain._dupes(prices, ["bucket_start", "item_id"]) == 0


def test_snapshot_mart_applies_tax_and_limits(run):
    rows = {r["item_id"]: r for r in run["ctx"].lake.read(SNAPSHOT).to_pylist()}
    bond, lobster, tbow = rows[13190], rows[379], rows.get(20997)
    assert bond["tax_exempt"] and bond["tax_per_item"] == 0
    assert lobster["tax_exempt"]
    if tbow and tbow["sell_price"]:
        assert tbow["tax_per_item"] == min(tbow["sell_price"] * 2 // 100, 5_000_000)
    for r in rows.values():
        if r["feasible_quantity"] is not None and r["buy_limit"] is not None:
            assert r["feasible_quantity"] <= r["buy_limit"]
        if r["net_margin"] is not None:
            assert r["net_margin"] <= r["sell_price"] - r["buy_price"]
        assert r["data_grade"] in "ABC" and r["data_grade_reason"]
        if r["is_idea"]:
            assert r["data_grade"] in "AB" and r["net_gp"] > 0


def test_rerun_is_a_no_op(run):
    ctx = make_context(run["root"])
    versions = {t: ctx.lake.table(t).version() for t in (ingest.PRICES, ingest.BUCKETS, SNAPSHOT)}
    assert tasks.hourly(ctx, FIXTURE_END) == 0
    assert ctx.client.transport.urls == []  # nothing missing, nothing re-fetched
    assert {t: ctx.lake.table(t).version() for t in versions} == versions


def test_forced_rebuild_reproduces_the_same_snapshot(run, monkeypatch):
    monkeypatch.setenv("GE_DIST_DIR", str(run["dist"]))
    ctx = make_context(run["root"])
    keys = ["snapshot_at", "item_id"]
    before = content_hash(ctx.lake.read(SNAPSHOT), keys)
    rows: list = []
    tasks.run_task(
        ctx, "build_snapshot", FIXTURE_END - timedelta(hours=1), FIXTURE_END, force=True, rows=rows
    )
    assert content_hash(ctx.lake.read(SNAPSHOT), keys) == before


def test_published_files_meet_the_v1_contract(run):
    data = run["dist"] / "ge" / "data" / "v1"
    snapshot = json.loads((data / "snapshot.json").read_text())
    status = json.loads((data / "status.json").read_text())
    publish.validate("snapshot", snapshot)
    publish.validate("status", status)
    assert snapshot["data_as_of"] == "2026-10-19T22:00:00Z"
    assert [i["rank"] for i in snapshot["ideas"]] == list(range(1, len(snapshot["ideas"]) + 1))
    assert all(
        a["net_gp"] >= b["net_gp"]
        for a, b in zip(snapshot["ideas"], snapshot["ideas"][1:], strict=False)
    )
    assert status["stale_after_minutes"] == 120
    assert (run["dist"] / "_headers").exists() and (run["dist"] / "ge" / "404.html").exists()
    assert "static.cloudflareinsights.com" in (run["dist"] / "_headers").read_text()


def test_contract_violations_are_refused(run):
    snapshot = json.loads((run["dist"] / "ge" / "data" / "v1" / "snapshot.json").read_text())
    broken = copy.deepcopy(snapshot)
    broken["ideas"][0]["data_grade"] = "C"  # grade C ideas are never published
    with pytest.raises(publish.ContractViolation):
        publish.validate("snapshot", broken)
    broken = copy.deepcopy(snapshot)
    broken["data_as_of"] = "2026-10-19 22:00"  # not UTC ISO 8601
    with pytest.raises(publish.ContractViolation):
        publish.validate("snapshot", broken)


def test_weekly_maintenance_keeps_everything_until_retention_is_accepted(run):
    rows = run["ctx"].lake.read(TASK_RUNS).to_pylist()
    detail = json.loads(next(r["detail"] for r in rows if r["task"] == "maintain"))
    assert not any(rule["applied"] for rule in detail["retention"].values())  # ADR-0019
    assert detail["tables"][SNAPSHOT]["files"] >= 1 and detail["tables"][SNAPSHOT]["bytes"] > 0


def test_tables_stay_on_the_s1_protocol_baseline(run):
    for name in (ingest.PRICES, ingest.BUCKETS, ingest.LATEST, ingest.ITEMS, SNAPSHOT, TASK_RUNS):
        protocol = run["ctx"].lake.table(name).protocol()
        assert (protocol.min_reader_version, protocol.min_writer_version) == (1, 2), name
        assert not protocol.reader_features and not protocol.writer_features, name
