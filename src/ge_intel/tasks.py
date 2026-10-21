"""The task registry and the Phase B scheduler (ADR-0004).

The registry is the single list of tasks, their upstream tasks and their cadence. The
`hourly` entry point (GitHub Actions now) and the Airflow DAGs (Phase C) both read it; each
task runs through `run_task`, which skips an interval already recorded as complete, times
the task and queues its `ops.task_runs` row.
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ge_intel import ingest, maintain, ops, publish
from ge_intel.build import build_snapshot
from ge_intel.ingest import Context
from ge_intel.timegrid import DAY, HOUR, floor, iso

# How far back the scheduler heals the live 5-minute series by itself (ADR-0018): 2,016
# buckets, cleared in about 11 hours at 96 a run and two runs an hour. An older gap is a
# backfill, which a person starts (`ge-intel run ingest_5m --start --end`); history before
# go-live lives in its own tables and never reaches this manifest.
LIVE_GAP_WINDOW = timedelta(days=7)


@dataclass(frozen=True)
class Task:
    name: str
    fn: Callable[[Context, datetime, datetime], dict]
    upstream: tuple[str, ...]
    cadence: str  # gap-fill | hourly | daily | weekly
    writes: tuple[str, ...]
    skip_if_complete: bool = True


REGISTRY: dict[str, Task] = {
    t.name: t
    for t in (
        Task("ingest_mapping", ingest.ingest_mapping, (), "daily", (ingest.ITEMS,)),
        Task(
            "ingest_5m",
            ingest.ingest_5m,
            (),
            "gap-fill",
            (ingest.PRICES, ingest.BUCKETS),
            skip_if_complete=False,  # completeness comes from the bucket manifest
        ),
        Task("ingest_latest", ingest.ingest_latest, (), "hourly", (ingest.LATEST,)),
        Task(
            "build_snapshot",
            build_snapshot,
            ("ingest_mapping", "ingest_5m", "ingest_latest"),
            "hourly",
            ("gold/market_snapshot",),
        ),
        Task("publish", publish.publish, ("build_snapshot",), "hourly", ()),
        Task("check_duplicates", maintain.check_duplicates, ("ingest_5m",), "daily", ()),
        Task("maintain", maintain.maintain, ("publish", "check_duplicates"), "weekly", ()),
    )
}


def log(event: str, **fields) -> None:
    print(json.dumps({"event": event, "at": iso(datetime.now(UTC)), **fields}, default=str))
    sys.stdout.flush()


def run_task(
    ctx: Context, name: str, start: datetime, end: datetime, *, force: bool, rows: list
) -> dict:
    task = REGISTRY[name]
    if end <= start:
        raise ValueError(f"empty interval {iso(start)} to {iso(end)}")
    if (
        task.skip_if_complete
        and not force
        and ops.is_complete(ctx.lake, ctx.cache, name, start, end)
    ):
        log("task_skipped", task=name, start=iso(start), end=iso(end), reason="already complete")
        return {"status": "complete"}
    started = datetime.now(UTC)
    t0 = time.monotonic()
    log("task_started", task=name, start=iso(start), end=iso(end))
    status, detail = "success", {}
    try:
        detail = task.fn(ctx, start, end) or {}
        status = detail.pop("status", "success")
        return {"status": status, **detail}
    except Exception as exc:
        status, detail = "failed", {"error": f"{type(exc).__name__}: {exc}"}
        raise
    finally:
        detail["duration_s"] = round(time.monotonic() - t0, 2)
        log("task_finished", task=name, status=status, **detail)
        rows.append(
            {
                "run_id": ctx.run_id,
                "task": name,
                "interval_start": start,
                "interval_end": end,
                "started_at": started,
                "finished_at": datetime.now(UTC),
                "status": status,
                "detail": detail,
                "code_version": ops.code_version(),
                "orchestrator": ops.orchestrator(),
            }
        )


def interval_for(ctx: Context, task: Task, end_hour: datetime) -> tuple[datetime, datetime]:
    """The interval the scheduler gives a task for the run whose newest closed hour ends at
    `end_hour`."""
    if task.cadence == "hourly":
        return end_hour - HOUR, end_hour
    if task.cadence == "daily":
        day = floor(end_hour, DAY)
        return day - DAY, day
    if task.cadence == "weekly":
        monday = floor(end_hour, DAY) - timedelta(days=end_hour.weekday())
        return monday - timedelta(days=7), monday
    if task.cadence == "gap-fill":
        # Everything since the first recorded bucket (the trailing day on a fresh lake, so the
        # first snapshot has its full 24-hour window), but never more than LIVE_GAP_WINDOW back.
        first = ingest.first_bucket(ctx)
        start = end_hour - timedelta(hours=24)
        if first is not None:
            start = min(first, start)
        return max(start, end_hour - LIVE_GAP_WINDOW), end_hour
    raise ValueError(task.cadence)


def hourly(
    ctx: Context, end_hour: datetime, *, force: bool = False, only: set[str] | None = None
) -> int:
    """Run every task in registry order for the hour ending at `end_hour`. Exit code."""
    rows: list[dict] = []
    started = datetime.now(UTC)
    outcome, failed_task = "success", None
    try:
        for name, task in REGISTRY.items():
            if only and name not in only:
                continue
            start, end = interval_for(ctx, task, end_hour)
            try:
                run_task(ctx, name, start, end, force=force, rows=rows)
            except Exception:
                traceback.print_exc()
                outcome, failed_task = "failed", name
                break
    finally:
        worked = [r for r in rows if r["status"] != "complete"]
        if worked:
            rows.append(
                {
                    "run_id": ctx.run_id,
                    "task": "hourly",
                    "interval_start": end_hour - HOUR,
                    "interval_end": end_hour,
                    "started_at": started,
                    "finished_at": datetime.now(UTC),
                    "status": outcome,
                    "detail": {"failed_task": failed_task, "commits": ctx.lake.commits},
                    "code_version": ops.code_version(),
                    "orchestrator": ops.orchestrator(),
                }
            )
            ops.flush(ctx.lake, rows)
        log("run_finished", outcome=outcome, end=iso(end_hour), failed_task=failed_task)
    return 0 if outcome == "success" else 1
