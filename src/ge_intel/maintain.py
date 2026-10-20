"""Data checks and table maintenance.

- `check_duplicates` (daily): no (item, bucket) pair appears twice in a day's partition of
  `silver/prices_5m`, and no bucket appears twice in the manifest. Duplicates can only arise
  within a bucket, so checking each day's partition once covers the whole table.
- `maintain` (weekly, ADR-0015 layer 2): compaction, `VACUUM` with 7-day retention in full
  mode (so files left by crashed writers go too, spike S1 rule 4), expired-log clean-up and a
  fresh checkpoint, one table at a time.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow.compute as pc

from ge_intel.build import SNAPSHOT
from ge_intel.ingest import BUCKETS, ITEMS, LATEST, PRICES, Context
from ge_intel.ops import TASK_RUNS

RETENTION_HOURS = 7 * 24
TABLES = (PRICES, BUCKETS, LATEST, ITEMS, SNAPSHOT, TASK_RUNS)


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


def maintain(ctx: Context, start: datetime, end: datetime) -> dict:
    done = {}
    for name in TABLES:
        dt = ctx.lake.table(name)
        if dt is None:
            continue
        metrics = dt.optimize.compact()
        removed = dt.vacuum(retention_hours=RETENTION_HOURS, dry_run=False, full=True)
        dt.cleanup_metadata()
        dt.create_checkpoint()
        done[name] = {
            "files_added": metrics.get("numFilesAdded"),
            "files_removed": metrics.get("numFilesRemoved"),
            "vacuumed": len(removed),
            "version": dt.version(),
        }
    return {"tables": done}
