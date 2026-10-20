"""Run records (`ops/task_runs`) and the freshness and completeness figures built from them.

Completion lives in storage, never only in the orchestrator (ADR-0004). Each process writes
its task rows in one commit when it exits, so a run costs one ops commit however many tasks
it ran, and nothing at all when every task was already complete.
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta

import pyarrow as pa

from ge_intel.lake import Lake
from ge_intel.timegrid import DAY, HOUR, floor, grid, iso

TASK_RUNS = "ops/task_runs"
ON_TIME = timedelta(minutes=45)  # AT4: published within 45 minutes of the hour closing

TS = pa.timestamp("us", tz="UTC")
RUNS_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("task", pa.string()),
        ("interval_start", TS),
        ("interval_end", TS),
        ("started_at", TS),
        ("finished_at", TS),
        ("status", pa.string()),  # success | skipped | failed | blocked
        ("detail", pa.string()),  # JSON
        ("code_version", pa.string()),
        ("orchestrator", pa.string()),
    ]
)


def run_id() -> str:
    gh = os.environ.get("GITHUB_RUN_ID")
    if gh:
        return f"gha-{gh}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return f"local-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{os.getpid()}"


def code_version() -> str:
    if os.environ.get("GITHUB_SHA"):
        return os.environ["GITHUB_SHA"]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=5
        )
        return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def orchestrator() -> str:
    return os.environ.get("GE_ORCHESTRATOR") or (
        "github-actions" if os.environ.get("GITHUB_ACTIONS") else "local"
    )


def runs(lake: Lake, cache: dict) -> list[dict]:
    if "runs" not in cache:
        table = lake.read(TASK_RUNS)
        cache["runs"] = table.to_pylist() if table is not None else []
    return cache["runs"]


def is_complete(lake: Lake, cache: dict, task: str, start: datetime, end: datetime) -> bool:
    return any(
        r["task"] == task
        and r["status"] == "success"
        and r["interval_start"] == start
        and r["interval_end"] == end
        for r in runs(lake, cache)
    )


def flush(lake: Lake, rows: list[dict]) -> None:
    if not rows:
        return
    table = pa.Table.from_pylist(
        [{**r, "detail": json.dumps(r.get("detail") or {}, default=str)} for r in rows],
        schema=RUNS_SCHEMA,
    )
    lake.write(
        TASK_RUNS, table, mode="append", metadata={"task": "ops", "run_id": rows[0]["run_id"]}
    )


# ---------------------------------------------------------------- status figures


def first_publish_times(publish_runs: list[dict]) -> dict[datetime, datetime]:
    """Hour end -> when that hour was first published live."""
    first: dict[datetime, datetime] = {}
    for r in publish_runs:
        end = r["interval_end"]
        if r["status"] == "success" and (end not in first or r["finished_at"] < first[end]):
            first[end] = r["finished_at"]
    return first


def due_hour_ends(
    go_live: datetime | None, now: datetime, start: datetime, end: datetime
) -> list[datetime]:
    """Hour ends in (start, end] from go-live on whose 45-minute deadline has passed."""
    if go_live is None:
        return []
    last_due = floor(now - ON_TIME, HOUR)
    lo = max(start, go_live - HOUR)
    return [e for e in grid(lo + HOUR, min(end, last_due) + HOUR, HOUR) if e >= go_live]


def freshness(hour_ends: list[datetime], first_publish: dict[datetime, datetime]) -> dict:
    """AT4: share of due hours published within 45 minutes of closing; unpublished counts late."""
    on_time = sum(1 for e in hour_ends if e in first_publish and first_publish[e] - e <= ON_TIME)
    published = sum(1 for e in hour_ends if e in first_publish)
    return {
        "hours_expected": len(hour_ends),
        "hours_on_time": on_time,
        "hours_published": published,
        "share_on_time": round(on_time / len(hour_ends), 4) if hour_ends else None,
    }


def completeness(
    recorded: dict[datetime, str], first_bucket: datetime | None, start: datetime, end: datetime
) -> dict:
    """Expected 5-minute buckets in [start, end) from the first bucket on, vs those recorded
    (traded or outage)."""
    if first_bucket is None:
        expected: list[datetime] = []
    else:
        expected = grid(max(start, first_bucket), end)
    present = [recorded[b] for b in expected if b in recorded]
    return {
        "buckets_expected": len(expected),
        "buckets_present": len(present),
        "outage_buckets": sum(s == "outage" for s in present),
        "share_present": round(len(present) / len(expected), 4) if expected else None,
    }


def status_figures(
    manifest_rows: list[dict],
    publish_runs: list[dict],
    data_end: datetime,
    now: datetime,
    days: int = 30,
) -> dict:
    """The 30-day freshness and completeness figures and the per-day breakdown.

    Completeness counts buckets up to the newest published hour (`data_end`); freshness counts
    hours whose 45-minute deadline has passed by `now`.
    """
    recorded = {r["bucket_start"]: r["status"] for r in manifest_rows}
    first_bucket = min(recorded) if recorded else None
    first_publish = first_publish_times(publish_runs)
    go_live = min(first_publish) if first_publish else None
    window_start = floor(now, DAY) - timedelta(days=days - 1)
    per_day = []
    for i in range(days):
        d0 = window_start + timedelta(days=i)
        d1 = d0 + DAY
        c = completeness(recorded, first_bucket, d0, min(d1, data_end))
        f = freshness(due_hour_ends(go_live, now, d0, d1), first_publish)
        if c["buckets_expected"] or f["hours_expected"]:
            per_day.append({"date": d0.date().isoformat(), **c, **f})
    return {
        "freshness_30d": freshness(due_hour_ends(go_live, now, window_start, now), first_publish),
        "completeness_30d": completeness(recorded, first_bucket, window_start, data_end),
        "days": per_day,
    }


def last_run(rows: list[dict], task: str, statuses=("success",)) -> dict | None:
    matching = [r for r in rows if r["task"] == task and r["status"] in statuses]
    return max(matching, key=lambda r: r["finished_at"]) if matching else None


def describe(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "status": row["status"],
        "interval_end": iso(row["interval_end"]),
        "finished_at": iso(row["finished_at"]),
    }
