"""`ge-intel`: the task interface (ADR-0004).

    ge-intel run <task> --start <UTC ISO-8601> --end <UTC ISO-8601> [--force]
    ge-intel hourly [--end <UTC hour>] [--force] [--only task,...]
    ge-intel guard
    ge-intel gaps [--start <UTC>] [--end <UTC>]
    ge-intel tasks
    ge-intel history plan|download|build|check-1h|fill|upload|pull|push [--dry-run] [--limit N]

Exit codes: 0 committed success; 1 a task failed; 2 R2 usage unreadable; 3 halted by the
cost guard; 4 a history step's inputs are not ready. Logs are JSON lines on stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

from ge_intel import cost_guard, history, ingest, kaggle, ops, tasks
from ge_intel.cost_guard import RunBudget
from ge_intel.ingest import Context
from ge_intel.lake import Lake
from ge_intel.timegrid import HOUR, floor, iso, parse_utc
from ge_intel.wiki import WikiClient

HISTORY_STEPS = ("plan", "download", "build", "check-1h", "fill", "upload", "pull", "push")


def context(budget: RunBudget | None = None) -> Context:
    budget = budget or RunBudget()
    return Context(
        lake=Lake.from_env(budget),
        client=WikiClient(),
        now=lambda: datetime.now(UTC),
        run_id=ops.run_id(),
    )


def previous_run(ctx: Context) -> cost_guard.PreviousRun | None:
    row = ops.last_run(ops.runs(ctx.lake, ctx.cache), "hourly", statuses=("success", "failed"))
    if row is None:
        return None
    return cost_guard.PreviousRun(row["run_id"], row["started_at"], row["finished_at"])


def guard(ctx: Context) -> int:
    """ADR-0015 layer 3 plus the previous-run check. Fails closed."""
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    token = os.environ.get("CLOUDFLARE_ANALYTICS_TOKEN")
    if not account_id or not token:
        tasks.log("guard", allowed=False, reasons=["analytics credentials not set"])
        return 2
    try:
        previous = previous_run(ctx)
        usage = cost_guard.fetch_usage(account_id, token, previous=previous)
    except Exception as exc:  # fail closed on any read failure
        tasks.log("guard", allowed=False, reasons=[f"cannot read R2 usage: {exc}"])
        return 2
    month = cost_guard.evaluate(usage)
    prev = cost_guard.check_previous_run(usage, previous, os.environ.get("GE_GUARD_ACK_RUN_ID"))
    allowed = month.allowed and prev.allowed
    tasks.log(
        "guard",
        allowed=allowed,
        class_a=usage.class_a,
        class_b=usage.class_b,
        storage_gb=round(usage.storage_bytes / 1e9, 3),
        previous_run=previous.run_id if previous else None,
        previous_window=usage.previous_window,
        reasons=month.reasons + prev.reasons,
        warnings=month.warnings + prev.warnings,
    )
    return 0 if allowed else 3


def run_history(step: str, dry_run: bool, limit: int | None) -> int:
    """The M2 history job (ADR-0018), run only on request: downloads, Wiki requests and the
    upload are Sylvester's to start, or an agent's he has asked (docs/kickoff/delegation_plan.md,
    ADR-0020). It runs on a throwaway GitHub runner (`history` workflow) or the laptop; `pull`
    and `push` keep its raw Wiki responses in `history/raw_wiki` between runs."""
    h = history.History(
        Path(os.environ.get("GE_HISTORY_DIR", "data/history")),
        client=WikiClient() if step in ("check-1h", "fill") else None,
    )
    try:
        if step == "plan":
            report = history.plan(h)
        elif step == "download":
            report = kaggle.download(h.dump, dry_run=dry_run, pins=h.pins)
        elif step == "build":
            report = history.build(h, dry_run=dry_run)
        elif step == "check-1h":
            report = history.check_1h(h, dry_run=dry_run)
        elif step == "fill":
            report = history.fill(h, dry_run=dry_run, limit=limit)
        elif step == "pull":
            report = history.pull_raw(h, Lake.from_env(RunBudget()))
        else:  # push or upload: writes to the lake, behind the cost guard on R2
            target = Lake.from_env(RunBudget())
            stored_bytes = None
            if target.root.startswith("s3://"):
                account = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
                token = os.environ.get("CLOUDFLARE_ANALYTICS_TOKEN")
                if not account or not token:
                    tasks.log("history", step=step, error="analytics credentials not set")
                    return 2
                usage = cost_guard.fetch_usage(account, token)
                verdict = cost_guard.evaluate(usage)
                if not verdict.allowed:
                    tasks.log("history", step=step, halted=list(verdict.reasons))
                    return 3
                stored_bytes = usage.storage_bytes
            if step == "push":
                report = history.push_raw(h, target, dry_run=dry_run)
            else:
                report = history.upload(h, target, dry_run=dry_run, stored_bytes=stored_bytes)
    except history.NotReady as exc:
        tasks.log("history", step=step, not_ready=str(exc))
        return 4
    tasks.log("history", step=step, dry_run=dry_run or step == "plan", report=report)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ge-intel")
    sub = ap.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run one task for a half-open UTC interval")
    run.add_argument("task", choices=sorted(tasks.REGISTRY))
    run.add_argument("--start", required=True, type=parse_utc)
    run.add_argument("--end", required=True, type=parse_utc)
    run.add_argument("--force", action="store_true", help="re-run even if recorded complete")
    hourly = sub.add_parser(
        "hourly", help="the scheduled run: every task for the newest closed hour"
    )
    hourly.add_argument("--end", type=parse_utc, help="hour end (default: the newest closed hour)")
    hourly.add_argument("--force", action="store_true")
    hourly.add_argument("--only", help="comma-separated task names")
    hourly.add_argument("--skip-guard", action="store_true", help="local runs on a local lake only")
    sub.add_parser("guard", help="R2 free-tier check (fails closed)")
    gaps = sub.add_parser("gaps", help="list missing 5-minute buckets")
    gaps.add_argument("--start", type=parse_utc, help="default: the scheduler's gap-fill window")
    gaps.add_argument("--end", type=parse_utc)
    sub.add_parser("tasks", help="print the task registry as JSON")
    hist = sub.add_parser(
        "history", help="M2 history job: the Kaggle dump, fills, upload; pull/push raw responses"
    )
    hist.add_argument("step", choices=HISTORY_STEPS)
    hist.add_argument("--dry-run", action="store_true", help="report only; send and write nothing")
    hist.add_argument("--limit", type=int, help="fill: at most this many buckets")
    args = ap.parse_args(argv)

    if args.cmd == "history":
        return run_history(args.step, args.dry_run, args.limit)

    if args.cmd == "tasks":
        print(
            json.dumps(
                {
                    n: {"upstream": t.upstream, "cadence": t.cadence, "writes": t.writes}
                    for n, t in tasks.REGISTRY.items()
                },
                indent=2,
            )
        )
        return 0

    ctx = context()
    if args.cmd == "guard":
        return guard(ctx)
    if args.cmd == "gaps":
        end = args.end or floor(datetime.now(UTC), HOUR)
        start = args.start or tasks.interval_for(ctx, tasks.REGISTRY["ingest_5m"], end)[0]
        missing = ingest.missing_buckets(ctx, start, end)
        print(
            json.dumps(
                {
                    "start": iso(start),
                    "end": iso(end),
                    "missing": len(missing),
                    "oldest": iso(min(missing)) if missing else None,
                }
            )
        )
        return 0
    if args.cmd == "run":
        rows: list[dict] = []
        try:
            tasks.run_task(ctx, args.task, args.start, args.end, force=args.force, rows=rows)
        except Exception:
            traceback.print_exc()
            return 1
        finally:
            ops.flush(ctx.lake, rows)
        return 0
    # hourly
    if args.skip_guard:
        if ctx.lake.root.startswith("s3://"):
            print("--skip-guard is only for local lakes", file=sys.stderr)
            return 2
    else:
        verdict = guard(ctx)
        if verdict:
            return verdict
    end = args.end or floor(datetime.now(UTC), HOUR)
    only = set(args.only.split(",")) if args.only else None
    return tasks.hourly(ctx, end, force=args.force, only=only)


if __name__ == "__main__":
    sys.exit(main())
