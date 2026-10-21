"""Data checks, table maintenance and retention.

- `check_duplicates` (daily): no (item, bucket) pair appears twice in a day's partition of
  `silver/prices_5m`, and no bucket appears twice in the manifest. Duplicates can only arise
  within a bucket, so checking each day's partition once covers the whole table.
- `maintain` (weekly, ADR-0015 layer 2): retention (ADR-0019), then compaction, `VACUUM` with
  7-day retention in full mode (so files left by crashed writers go too, spike S1 rule 4),
  expired-log clean-up and a fresh checkpoint, one table at a time. It reports each table's
  files and bytes from the Delta log, so storage can be traced per table without reading data.

Retention runs here because the hourly workflow is the only writer of the live tables. It
deletes through delta-rs, never through an R2 lifecycle rule (which would delete files the
Delta log still lists), and never touches `history/` (ADR-0018). Deleting data in R2 is
Sylvester's decision: until ADR-0019 is accepted, `RETENTION` keeps everything and each weekly
run only reports what `PROPOSED` would remove.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ge_intel.build import SNAPSHOT
from ge_intel.ingest import BUCKETS, ITEMS, LATEST, PRICES, PRICES_SCHEMA, Context
from ge_intel.lake import ZSTD
from ge_intel.ops import TASK_RUNS

RETENTION_HOURS = 7 * 24
TABLES = (PRICES, BUCKETS, LATEST, ITEMS, SNAPSHOT, TASK_RUNS)


@dataclass(frozen=True)
class Retention:
    """What the weekly maintenance keeps in the live tables (ADR-0019). None or False keeps
    everything."""

    snapshot_full_days: int | None  # gold/market_snapshot: rows that are not ideas go after
    latest_days: int | None  # silver/latest_snapshots: whole days go after
    rewrite_closed_months: bool  # silver/prices_5m: closed months to zstd, sorted by item


KEEP_ALL = Retention(snapshot_full_days=None, latest_days=None, rewrite_closed_months=False)
PROPOSED = Retention(snapshot_full_days=30, latest_days=30, rewrite_closed_months=True)
RETENTION = KEEP_ALL  # until Sylvester accepts ADR-0019
RETAINED = (SNAPSHOT, LATEST, PRICES)
# A mart day loses its non-idea rows in the weekly run after its age passes the limit; the run
# also re-checks the four weeks before, so a paused week catches up while the scan for idea
# rows stays bounded (five weeks of day partitions). A longer pause: `ge-intel run maintain`
# with an earlier --start (runbook).
SWEEP = timedelta(days=28)
# A month is closed once the scheduler can no longer write to it: its last day plus the live
# gap-fill window (tasks.LIVE_GAP_WINDOW; a test keeps the two in step).
CLOSED_AFTER = timedelta(days=7)


class DuplicateRows(RuntimeError):
    pass


def _dupes(table, keys: list[str]) -> int:
    if table is None or table.num_rows == 0:
        return 0
    counts = table.group_by(keys).aggregate([([], "count_all")])
    return pc.sum(pc.greater(counts.column("count_all"), 1)).as_py() or 0


def check_duplicates(ctx: Context, start: datetime, end: datetime) -> dict:
    days = []
    d = start.date()
    while d < end.date():
        days.append(d)
        d = d.fromordinal(d.toordinal() + 1)
    prices = ctx.lake.read(
        PRICES, columns=["bucket_start", "item_id"], filters=[("bucket_date", "in", days)]
    )
    manifest = ctx.lake.read(BUCKETS, columns=["bucket_start"])
    found = {
        "prices_5m": _dupes(prices, ["bucket_start", "item_id"]),
        "wiki_5m_buckets": _dupes(manifest, ["bucket_start"]),
    }
    rows = prices.num_rows if prices is not None else 0
    if any(found.values()):
        raise DuplicateRows(f"duplicate keys found: {found}")
    return {"rows_checked": rows, "duplicate_keys": found}


# ---------------------------------------------------------------- retention (ADR-0019)


def active_files(dt, partition: str | None = None) -> list[dict]:
    """The table's live files from its Delta log (no data read): path, bytes, rows and the
    partition value."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    if actions.num_rows == 0:
        return []
    parts = (
        actions.column(f"partition.{partition}").to_pylist()
        if partition
        else [None] * actions.num_rows
    )
    return [
        {"path": path, "bytes": size, "rows": rows or 0, "part": part}
        for path, size, rows, part in zip(
            actions.column("path").to_pylist(),
            actions.column("size_bytes").to_pylist(),
            actions.column("num_records").to_pylist(),
            parts,
            strict=True,
        )
    ]


def _summary(files: list[dict]) -> dict:
    return {
        "days": len({f["part"] for f in files}),
        "files": len(files),
        "bytes": sum(f["bytes"] for f in files),
        "rows": sum(f["rows"] for f in files),
    }


def trim_snapshot(
    ctx: Context, days: int, start: datetime, end: datetime, *, dry_run: bool
) -> dict:
    """Keep every idea row of `gold/market_snapshot` (the paper record, AT7) and the full mart
    for `days`: rows that are not ideas go once their day is older. A grade A or B row without
    a positive net GP has `is_idea` null, so the test is `IS NOT TRUE`. Each day's file is
    rewritten with its idea rows only (Class A writes, a handful a week)."""
    dt = ctx.lake.table(SNAPSHOT)
    if dt is None:
        return {}
    cutoff = (end - timedelta(days=days)).date()
    floor = (start - timedelta(days=days) - SWEEP).date()
    scope = [f for f in active_files(dt, "snapshot_date") if floor <= f["part"] < cutoff]
    out = {"from": floor.isoformat(), "before": cutoff.isoformat(), **_summary(scope)}
    if dry_run or not scope:
        return out
    metrics = ctx.lake.delete(
        SNAPSHOT,
        f"snapshot_date >= '{floor}' AND snapshot_date < '{cutoff}' AND is_idea IS NOT TRUE",
        metadata=ctx.metadata("maintain", start, end),
    )
    return {
        **out,
        "rows_deleted": metrics["num_deleted_rows"],
        "files_rewritten": metrics["num_added_files"],
    }


def expire_latest(
    ctx: Context, days: int, start: datetime, end: datetime, *, dry_run: bool
) -> dict:
    """Drop whole days of `silver/latest_snapshots` older than `days`. The predicate is on the
    partition column only, so no file is rewritten; the files go at the VACUUM a week later
    (deletes are free)."""
    dt = ctx.lake.table(LATEST)
    if dt is None:
        return {}
    cutoff = (end - timedelta(days=days)).date()
    scope = [f for f in active_files(dt, "snapshot_date") if f["part"] < cutoff]
    out = {"before": cutoff.isoformat(), **_summary(scope)}
    if dry_run or not scope:
        return out
    metrics = ctx.lake.delete(
        LATEST, f"snapshot_date < '{cutoff}'", metadata=ctx.metadata("maintain", start, end)
    )
    return {
        **out,
        "rows_deleted": metrics["num_deleted_rows"],
        "files_rewritten": metrics["num_added_files"],
    }


def month_bounds(month: str) -> tuple[date, date]:
    first = date(int(month[:4]), int(month[5:7]), 1)
    return first, (first + timedelta(days=32)).replace(day=1)


def pending_months(files: list[dict], end: datetime) -> dict[str, list[dict]]:
    """Closed months of `silver/prices_5m` not yet in their final form (one zstd file a day),
    oldest first. delta-rs names a zstd file `*.zstd.parquet`; a test pins that."""
    months: dict[str, dict[date, list[dict]]] = {}
    for f in files:
        months.setdefault(f["part"].strftime("%Y-%m"), {}).setdefault(f["part"], []).append(f)
    pending = {}
    for month, by_day in sorted(months.items()):
        nxt = month_bounds(month)[1]
        if datetime(nxt.year, nxt.month, nxt.day, tzinfo=UTC) + CLOSED_AFTER > end:
            continue
        if all(len(fs) == 1 and fs[0]["path"].endswith(".zstd.parquet") for fs in by_day.values()):
            continue
        pending[month] = [f for fs in by_day.values() for f in fs]
    return pending


def rewrite_closed_month(ctx: Context, start: datetime, end: datetime, *, dry_run: bool) -> dict:
    """Rewrite the oldest pending closed month of `silver/prices_5m` in zstd, each day sorted
    by item (measured on a real day: 10.3 bytes a row as written, 7.74 after). The rows do not
    change. One commit for the month, one month a run, so a run's requests stay bounded (about
    31 file writes; ADR-0015 caps). Each day is sorted in memory and spilled to a local file
    first: delta-rs cannot read a table from inside its own write. A month that later gains a
    file (a backfill) is pending again and rewritten again."""
    dt = ctx.lake.table(PRICES)
    if dt is None:
        return {}
    pending = pending_months(active_files(dt, "bucket_date"), end)
    if not pending:
        return {"pending_months": 0}
    month, files = next(iter(pending.items()))
    out = {"month": month, "pending_months": len(pending), **_summary(files)}
    if dry_run:
        return out
    first, nxt = month_bounds(month)
    with tempfile.TemporaryDirectory(prefix="ge-month-") as tmp:
        spilled = []
        for day in sorted({f["part"] for f in files}):
            rows = dt.to_pyarrow_table(filters=[("bucket_date", "=", day)])
            rows = rows.select(PRICES_SCHEMA.names).cast(PRICES_SCHEMA)
            rows = rows.sort_by([("item_id", "ascending"), ("bucket_start", "ascending")])
            spilled.append(Path(tmp) / f"{day}.parquet")
            pq.write_table(rows, spilled[-1])
        batches = (b for path in spilled for b in pq.ParquetFile(path).iter_batches())
        ctx.lake.write(
            PRICES,
            pa.RecordBatchReader.from_batches(PRICES_SCHEMA, batches),
            predicate=f"bucket_date >= '{first}' AND bucket_date < '{nxt}'",
            partition_by=["bucket_date"],
            metadata=ctx.metadata("maintain", start, end),
            writer_properties=ZSTD,
        )
    return out


def apply_retention(ctx: Context, policy: Retention, start: datetime, end: datetime) -> dict:
    """Run each rule the policy turns on; dry-run each rule it leaves off at `PROPOSED`'s
    setting, so the weekly log shows what accepting ADR-0019 would remove."""
    full, latest = policy.snapshot_full_days, policy.latest_days
    return {
        SNAPSHOT: {
            "applied": full is not None,
            **trim_snapshot(
                ctx, full or PROPOSED.snapshot_full_days, start, end, dry_run=full is None
            ),
        },
        LATEST: {
            "applied": latest is not None,
            **expire_latest(
                ctx, latest or PROPOSED.latest_days, start, end, dry_run=latest is None
            ),
        },
        PRICES: {
            "applied": policy.rewrite_closed_months,
            **rewrite_closed_month(ctx, start, end, dry_run=not policy.rewrite_closed_months),
        },
    }


def maintain(ctx: Context, start: datetime, end: datetime) -> dict:
    retention = apply_retention(ctx, RETENTION, start, end)  # first, so compaction sees it
    done = {}
    for name in TABLES:
        dt = ctx.lake.table(name)
        if dt is None:
            continue
        metrics = dt.optimize.compact()
        removed = dt.vacuum(retention_hours=RETENTION_HOURS, dry_run=False, full=True)
        dt.cleanup_metadata()
        dt.create_checkpoint()
        files = active_files(dt)
        done[name] = {
            "files_added": metrics.get("numFilesAdded"),
            "files_removed": metrics.get("numFilesRemoved"),
            "vacuumed": len(removed),
            "version": dt.version(),
            "files": len(files),
            "bytes": sum(f["bytes"] for f in files),
        }
    return {"tables": done, "retention": retention}
