"""Freshness (AT4) and completeness figures, and the scheduler's intervals."""

from datetime import UTC, datetime, timedelta

from conftest import make_context

from ge_intel import ops, tasks
from ge_intel.timegrid import FIVE_MIN, HOUR, grid

GO_LIVE = datetime(2026, 10, 20, 0, tzinfo=UTC)


def publish(end, minutes_late, status="success"):
    return {
        "task": "publish",
        "interval_end": end,
        "finished_at": end + timedelta(minutes=minutes_late),
        "status": status,
    }


def test_freshness_counts_late_and_missing_hours_against_the_objective():
    now = GO_LIVE + timedelta(hours=10, minutes=50)  # hours ending 01:00..10:00 are due
    runs = [publish(GO_LIVE + i * HOUR, 20) for i in range(1, 11)]
    runs[3] = publish(GO_LIVE + 4 * HOUR, 50)  # late
    del runs[5]  # never published (hour ending 06:00)
    runs.append(publish(GO_LIVE + 7 * HOUR, 5, status="built"))  # built, not deployed: ignored
    hours = ops.due_hour_ends(GO_LIVE + HOUR, now, GO_LIVE, now)
    f = ops.freshness(hours, ops.first_publish_times(runs))
    assert f == {
        "hours_expected": 10,
        "hours_on_time": 8,
        "hours_published": 9,
        "share_on_time": 0.8,
    }


def test_hours_not_yet_due_are_not_counted():
    now = GO_LIVE + timedelta(hours=1, minutes=30)  # the 01:00 hour is due at 01:45
    assert ops.due_hour_ends(GO_LIVE + HOUR, now, GO_LIVE, now) == []


def test_completeness_counts_outages_as_recorded():
    start = GO_LIVE
    recorded = {b: "ok" for b in grid(start, start + HOUR)}
    recorded[start + 2 * FIVE_MIN] = "outage"
    del recorded[start + 5 * FIVE_MIN]
    c = ops.completeness(recorded, start, start, start + HOUR)
    assert c == {
        "buckets_expected": 12,
        "buckets_present": 11,
        "outage_buckets": 1,
        "share_present": 0.9167,
    }


def test_status_figures_break_down_by_day():
    manifest = [{"bucket_start": b, "status": "ok"} for b in grid(GO_LIVE, GO_LIVE + 26 * HOUR)]
    runs = [publish(GO_LIVE + i * HOUR, 10) for i in range(1, 27)]
    now = GO_LIVE + timedelta(hours=26, minutes=50)
    figs = ops.status_figures(manifest, runs, GO_LIVE + 26 * HOUR, now)
    assert [d["date"] for d in figs["days"]] == ["2026-10-20", "2026-10-21"]
    assert figs["days"][0]["buckets_present"] == 288 and figs["days"][0]["hours_expected"] == 24
    assert figs["freshness_30d"]["share_on_time"] == 1.0
    assert figs["completeness_30d"]["share_present"] == 1.0


def test_scheduler_intervals_follow_each_cadence(tmp_path):
    ctx = make_context(tmp_path)
    end = datetime(2026, 10, 21, 0, tzinfo=UTC)  # a Wednesday
    reg = tasks.REGISTRY
    assert tasks.interval_for(ctx, reg["publish"], end) == (end - HOUR, end)
    assert tasks.interval_for(ctx, reg["ingest_mapping"], end) == (end - timedelta(days=1), end)
    monday = datetime(2026, 10, 19, tzinfo=UTC)
    assert tasks.interval_for(ctx, reg["maintain"], end) == (monday - timedelta(days=7), monday)
    assert tasks.interval_for(ctx, reg["ingest_5m"], end) == (end - timedelta(hours=24), end)


def test_registry_upstreams_exist_and_come_first():
    names = list(tasks.REGISTRY)
    for i, (name, task) in enumerate(tasks.REGISTRY.items()):
        for up in task.upstream:
            assert up in names[:i], f"{name} runs before its upstream {up}"
