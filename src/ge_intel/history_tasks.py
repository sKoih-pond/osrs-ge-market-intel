"""History and reprocessing as tasks on the contract (ADR-0004, amended): run on request only,
by a backfill DAG or a person, never by the hourly run.

    ge-intel run history_build_1h --start 2026-01-01T00:00:00Z --end 2026-02-01T00:00:00Z

Every interval is whole UTC months, and the tasks that build or compare take exactly one
month: the partition a backfill maps over. They read the history job's inputs on local disk
(`GE_HISTORY_DIR`) and never ask the Wiki; only `history_pull` and `history_r2_check` read R2,
and nothing here writes to it. Their run records go to the lake named by `GE_LAKE_ROOT`, which
must be a local directory (the CLI refuses R2), so a backfill never becomes a second writer of
the live `ops/task_runs`.
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from ge_intel import history, kaggle, parity
from ge_intel.cost_guard import RunBudget
from ge_intel.ingest import Context
from ge_intel.lake import BUCKET, Lake, r2_storage_options
from ge_intel.objects import LocalStore, R2Store
from ge_intel.tasks import Task


class ParityFailed(RuntimeError):
    """Two builds of a month are not equivalent, or the local build disagrees with R2."""


def months_in(start: datetime, end: datetime) -> list[str]:
    """The whole UTC months in `[start, end)`; any other interval is refused."""
    for moment in (start, end):
        if (moment.day, moment.hour, moment.minute, moment.second, moment.microsecond) != (
            1,
            0,
            0,
            0,
            0,
        ):
            raise ValueError(f"{history.iso(moment)} is not the start of a UTC month")
    out, cur = [], start.astimezone(UTC)
    while cur < end:
        out.append(history.month_of(cur))
        cur = history.month_bounds(out[-1])[1]
    if not out:
        raise ValueError("empty interval")
    return out


def one_month(start: datetime, end: datetime) -> str:
    months = months_in(start, end)
    if len(months) != 1:
        raise ValueError(f"this task takes one month at a time, got {len(months)}")
    return months[0]


def local_history() -> history.History:
    """The history directory, with no Wiki client: these tasks never send a request."""
    return history.History(Path(os.environ.get("GE_HISTORY_DIR", "data/history")))


def source_lake() -> Lake:
    """Where the stored Wiki responses and the published history live: R2, read only here
    (`GE_HISTORY_SOURCE` names a local lake instead, for tests)."""
    root = os.environ.get("GE_HISTORY_SOURCE", f"s3://{BUCKET}")
    if not root.startswith("s3://"):
        return Lake(root=root, store=LocalStore(root))
    account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    kid, secret = os.environ["R2_ACCESS_KEY_ID"], os.environ["R2_SECRET_ACCESS_KEY"]
    bucket = root.removeprefix("s3://")
    return Lake(
        root=root,
        store=R2Store(account, bucket, kid, secret, RunBudget()),
        storage_options=r2_storage_options(account, kid, secret),
    )


def spark_lake(h: history.History) -> Path:
    return Path(os.environ.get("GE_SPARK_LAKE", h.root / "spark"))


# ---------------------------------------------------------------- the tasks


def download(ctx: Context, start: datetime, end: datetime) -> dict:
    """The pinned dump files for the interval's months: checked against their pins, fetched
    from Kaggle only if missing."""
    h = local_history()
    months = set(months_in(start, end))
    pins = [pin for month, pin in h.month_pins().items() if month in months]
    if len(pins) != len(months):
        raise ValueError(f"no pinned dump file for {sorted(months - set(h.month_pins()))}")
    report = kaggle.download(h.dump, pins=pins)
    return {"months": len(pins), **{k: v for k, v in report.items() if isinstance(v, int)}}


def pull(ctx: Context, start: datetime, end: datetime) -> dict:
    """The stored Wiki responses for buckets in the interval, restored from R2 (a read)."""
    months_in(start, end)
    return history.pull_raw(local_history(), source_lake(), start, end)


def build_1h(ctx: Context, start: datetime, end: datetime) -> dict:
    """One month of `history/prices_1h` with DuckDB, as the history job builds it."""
    return history.build_hourly(local_history(), one_month(start, end))


def reprocess_1h(ctx: Context, start: datetime, end: datetime) -> dict:
    """One month of `history/prices_1h` with PySpark, in the engines' own environment (the
    command named by `GE_SPARK`)."""
    month = one_month(start, end)
    h = local_history()
    command = os.environ.get("GE_SPARK", "engines/.venv/bin/ge-spark")
    done = subprocess.run(
        [
            command,
            "reprocess",
            "--months",
            month,
            "--history-dir",
            str(h.root),
            "--out",
            str(spark_lake(h)),
            "--live-start",
            history.iso(h.live_start),
            "--threads",
            os.environ.get("GE_SPARK_THREADS", "2"),
            "--memory",
            os.environ.get("GE_SPARK_MEMORY", "6g"),
            "--local-dir",
            str(h.root / "tmp" / f"spark-{month}"),
        ],
        capture_output=True,
        text=True,
    )
    if done.returncode != 0:
        raise RuntimeError(f"ge-spark failed ({done.returncode}): {done.stderr[-2000:]}")
    report = json.loads(done.stdout.strip().splitlines()[-1])
    return {
        "month": month,
        "rows": report["rows"],
        "replaced": report["repaired"].get(month, {}).get("replaced", []),
        "spark_s": report["total_s"],
        "session_start_s": report["session_start_s"],
    }


def parity_1h(ctx: Context, start: datetime, end: datetime) -> dict:
    """The DuckDB and PySpark builds of one month, compared (`parity.compare`). Fails unless
    they are equivalent; a one-step rounding flip is reported, not failed."""
    month = one_month(start, end)
    h = local_history()
    con = parity.connect(h.root / "tmp" / f"parity-{month}")
    parity.register_delta(con, "duckdb_build", h.lake.uri(history.PRICES_1H))
    parity.register_delta(con, "spark_build", str(spark_lake(h) / history.PRICES_1H))
    found = parity.compare(con, "duckdb_build", "spark_build", [month])
    con.close()
    result = found["by_month"][month]
    if not result["equivalent"] or result["rows_a"] == 0:
        raise ParityFailed(f"{month}: {result}")
    return {
        "month": month,
        **{
            k: result[k] for k in ("rows_a", "rows_b", "exact", "rounding_flips", "checksums_equal")
        },
    }


def r2_check(ctx: Context, start: datetime, end: datetime) -> dict:
    """The local builds' row counts for the interval against R2's own record: the Delta log of
    `history/prices_1h` and the months manifest. Reads R2's log only, no data."""
    months = months_in(start, end)
    h = local_history()
    r2 = source_lake()
    counts, manifest = parity.lake_counts(r2), parity.months_manifest(r2)
    local = parity.lake_counts(h.lake)
    built = {m: local.get(m) for m in months}
    published = {m: counts.get(m) for m in months}
    recorded = {m: manifest.get(m, {}).get("hourly_rows") for m in months}
    if not (built == published == recorded):
        differ = [m for m in months if not (built[m] == published[m] == recorded[m])]
        raise ParityFailed(f"local build and R2 disagree on {differ[:12]}")
    return {"months": len(months), "rows": sum(built.values())}


# Run on request only: `tasks.hourly` iterates `tasks.REGISTRY`, never this list. Writes name
# the local tables; none is in R2.
REGISTRY: dict[str, Task] = {
    t.name: t
    for t in (
        Task("history_download", download, (), "on-request", ()),
        Task("history_pull", pull, (), "on-request", ()),
        Task(
            "history_build_1h",
            build_1h,
            ("history_download", "history_pull"),
            "on-request",
            (history.PRICES_1H, history.PRICES_5M),
        ),
        Task(
            "reprocess_1h",
            reprocess_1h,
            ("history_download", "history_pull"),
            "on-request",
            ("spark:" + history.PRICES_1H,),
        ),
        Task("parity_1h", parity_1h, ("history_build_1h", "reprocess_1h"), "on-request", ()),
        Task("history_r2_check", r2_check, ("history_build_1h",), "on-request", ()),
    )
}
