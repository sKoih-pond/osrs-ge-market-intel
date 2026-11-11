"""`build_snapshot`: run the dbt-duckdb mart for one hour, then write it to gold with delta-rs.

dbt builds into a throwaway DuckDB file on the runner; the gold table is written by delta-rs
with replace-where on the snapshot hour, never by DuckDB `INSERT` (spike S1 rule 3), so a
re-run replaces the hour instead of appending a copy. A forced rebuild of an hour older than the
newest closed hour is refused when it would regrade the hour or overwrite its idea rows
(`refuse_old_rebuild`, ADR-0019 amendment of 12 November 2026).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from ge_intel.ingest import BUCKETS, LATEST, Context
from ge_intel.policies import CASUAL_V1, DATA_GRADE_V2
from ge_intel.timegrid import FIVE_MIN, HOUR, grid, iso, sql_ts

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
SNAPSHOT = "gold/market_snapshot"
WINDOW = timedelta(hours=24)


class UpstreamIncomplete(RuntimeError):
    """The hour's buckets are not all recorded yet; the next run will catch up first."""


class RebuildRefused(RuntimeError):
    """A forced rebuild of an old hour would regrade it or overwrite its idea rows."""


def hour_scope(end: datetime) -> tuple[str, list[tuple]]:
    """The rows one build replaces, as the write's replace-where predicate and as read filters."""
    predicate = f"snapshot_date = '{end.date().isoformat()}' AND snapshot_at = '{sql_ts(end)}'"
    filters = [("snapshot_date", "=", end.date()), ("snapshot_at", "=", end)]
    return predicate, filters


def dbt_vars(lake_root: str, end: datetime) -> dict:
    start = end - WINDOW
    return {
        "lake_root": lake_root,
        "snapshot_at": sql_ts(end),
        "window_start": sql_ts(start),
        "first_date": start.date().isoformat(),
        "last_date": end.date().isoformat(),
        "policy": CASUAL_V1.as_vars(),
        "grade": DATA_GRADE_V2.as_vars(),
    }


def run_dbt(lake_root: str, end: datetime, workdir: Path) -> Path:
    """`dbt build` (seeds, models, tests) in its own process, so its DuckDB file is closed
    before this process reads the mart."""
    db_path = workdir / "ge_intel.duckdb"
    target = "r2" if lake_root.startswith("s3://") else "local"
    dbt = Path(sys.executable).with_name("dbt")
    result = subprocess.run(
        [
            str(dbt),
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target",
            target,
            "--target-path",
            str(workdir / "target"),
            "--log-path",
            str(workdir / "logs"),
            "--vars",
            json.dumps(dbt_vars(lake_root, end)),
            "--no-use-colors",
        ],
        env={**os.environ, "GE_DBT_DUCKDB_PATH": str(db_path), "DO_NOT_TRACK": "1"},
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        tail = "\n".join((result.stdout + result.stderr).strip().splitlines()[-25:])
        raise RuntimeError(f"dbt build failed:\n{tail}")
    return db_path


def require_hour_recorded(ctx: Context, start: datetime, end: datetime) -> None:
    table = ctx.lake.read(
        BUCKETS,
        columns=["bucket_start"],
        filters=[("bucket_start", ">=", start), ("bucket_start", "<", end)],
    )
    have = set(table.column("bucket_start").to_pylist()) if table is not None else set()
    missing = [b for b in grid(start, end, FIVE_MIN) if b not in have]
    if missing:
        raise UpstreamIncomplete(
            f"{len(missing)} buckets of {iso(start)} to {iso(end)} not recorded"
        )


def refuse_old_rebuild(ctx: Context, start: datetime, end: datetime) -> None:
    """Refuse a forced rebuild of an hour before the newest closed hour (`run_task` calls this
    only then) if its /latest rows are gone or it already holds idea rows.

    /latest has no timestamp parameter, so an hour's rows are never fetched again, and rule B1
    (ADR-0019) drops them after 30 days. Without them the mart has no minutes since the last
    trade, and a rebuild silently regrades the hour (a scratch run went from A17/B7/C42 to
    A0/B26/C40). The idea rows are the paper record (AT7) and outlive the rest of the hour's
    rows (rule A1), so a rebuild must not replace them either. The window is `stg_latest`'s."""
    reasons = []
    latest = ctx.lake.read(
        LATEST,
        columns=["snapshot_at"],
        filters=[
            ("snapshot_date", ">=", (end - HOUR).date()),
            ("snapshot_date", "<=", end.date()),
            ("snapshot_at", ">=", end - HOUR),
            ("snapshot_at", "<=", end),
        ],
    )
    if latest is None or latest.num_rows == 0:
        reasons.append("its /latest rows are gone, so the rebuild would regrade it")
    ideas = ctx.lake.read(
        SNAPSHOT, columns=["item_id"], filters=[*hour_scope(end)[1], ("is_idea", "=", True)]
    )
    if ideas is not None and ideas.num_rows:
        reasons.append(f"it holds {ideas.num_rows} idea rows, the paper record (AT7)")
    if reasons:
        raise RebuildRefused(
            f"refused: a forced rebuild of the hour ending {iso(end)}, older than the newest "
            f"closed hour: {'; '.join(reasons)} (docs/runbook.md, Storage and retention)"
        )


def build_snapshot(ctx: Context, start: datetime, end: datetime) -> dict:
    require_hour_recorded(ctx, end - timedelta(hours=CASUAL_V1.price_window_hours), end)
    with tempfile.TemporaryDirectory(prefix="ge-dbt-") as tmp:
        db_path = run_dbt(ctx.lake.root, end, Path(tmp))
        with duckdb.connect(str(db_path), read_only=True) as con:
            con.execute("SET TimeZone = 'UTC'")
            table = con.sql(
                "select *, data_grade in ('A', 'B') and net_gp > 0 as is_idea, "
                "case when data_grade in ('A', 'B') and net_gp > 0 then "
                "row_number() over (partition by data_grade in ('A', 'B') and net_gp > 0 "
                "order by net_gp desc, item_id) end as idea_rank "
                "from main.market_snapshot order by item_id"
            ).arrow()
    if hasattr(table, "read_all"):
        table = table.read_all()
    ctx.lake.write(
        SNAPSHOT,
        table,
        predicate=hour_scope(end)[0],
        partition_by=["snapshot_date"],
        metadata=ctx.metadata("build_snapshot", start, end),
        schema_mode="merge",  # the mart may gain columns, e.g. a new grade input
    )
    grades = {g: table.column("data_grade").to_pylist().count(g) for g in "ABC"}
    return {
        "rows_written": table.num_rows,
        "ideas": sum(bool(v) for v in table.column("is_idea").to_pylist()),
        "grades": grades,
    }
