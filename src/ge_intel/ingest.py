"""Ingestion tasks: `/5m` buckets, hourly `/latest`, daily `/mapping` (ADR-0005).

Each task takes a half-open UTC interval and is safe to re-run (ADR-0004):
- `ingest_5m` fetches only the closed buckets in the interval that the bucket manifest does
  not yet hold, newest first, up to a per-run cap; every gap is filled by this or a later run.
- `/latest` and `/mapping` have no timestamp parameter, so they can only be snapshotted
  close to the interval they describe; asked about an older interval, the task records a skip
  instead of storing today's state under yesterday's date.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import statistics
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from ge_intel import wiki
from ge_intel.lake import Lake, in_list
from ge_intel.reference import regime_at
from ge_intel.timegrid import FIVE_MIN, coalesce_ranges, days_touched, grid, iso, sql_ts

PRICES = "silver/prices_5m"
BUCKETS = "silver/wiki_5m_buckets"
LATEST = "silver/latest_snapshots"
ITEMS = "silver/item_dim"

MAX_BUCKETS_PER_RUN = 96  # 8 hours: about 100 s of requests and 100 PutObject (ADR-0015 caps)
# A bucket with no items that closed this recently may simply not be published yet.
UNPUBLISHED_GRACE = timedelta(minutes=15)
# Outage rule (ADR-0005): item count below this share of the trailing 24-hour median.
# Derivation in docs/methodology/outage_rule.md.
OUTAGE_RATIO = 0.6
OUTAGE_MIN_HISTORY = 12
TRAILING_BUCKETS = 288
LIVE_SOURCE_MAX_AGE = timedelta(hours=1)

TS = pa.timestamp("us", tz="UTC")
PRICES_SCHEMA = pa.schema(
    [
        ("bucket_start", TS),
        ("bucket_date", pa.date32()),
        ("item_id", pa.int32()),
        ("avg_high_price", pa.float64()),
        ("high_volume", pa.int64()),
        ("avg_low_price", pa.float64()),
        ("low_volume", pa.int64()),
        ("regime_id", pa.string()),
        ("raw_sha256", pa.string()),
    ]
)
BUCKETS_SCHEMA = pa.schema(
    [
        ("bucket_start", TS),
        ("bucket_date", pa.date32()),
        ("item_count", pa.int32()),
        ("trailing_median_items", pa.float64()),
        ("status", pa.string()),  # ok | outage
        ("regime_id", pa.string()),
        ("raw_key", pa.string()),
        ("raw_sha256", pa.string()),
        ("fetched_at", TS),
    ]
)
LATEST_SCHEMA = pa.schema(
    [
        ("snapshot_at", TS),
        ("snapshot_date", pa.date32()),
        ("fetched_at", TS),
        ("item_id", pa.int32()),
        ("high", pa.int64()),
        ("high_time", TS),
        ("low", pa.int64()),
        ("low_time", TS),
        ("raw_sha256", pa.string()),
    ]
)
ITEMS_SCHEMA = pa.schema(
    [
        ("item_id", pa.int32()),
        ("name", pa.string()),
        ("examine", pa.string()),
        ("members", pa.bool_()),
        ("lowalch", pa.int64()),
        ("highalch", pa.int64()),
        ("value", pa.int64()),
        ("buy_limit", pa.int64()),
        ("icon", pa.string()),
        ("row_hash", pa.string()),
        ("valid_from", pa.date32()),
        ("valid_to", pa.date32()),
        ("is_current", pa.bool_()),
    ]
)


@dataclass
class Context:
    lake: Lake
    client: wiki.WikiClient
    now: callable
    run_id: str
    max_buckets: int = MAX_BUCKETS_PER_RUN
    cache: dict = field(default_factory=dict)
    # Wall-clock time after which a run starts no new work (`tasks.RUN_BUDGET`); None: no limit.
    deadline: datetime | None = None

    def out_of_time(self) -> bool:
        return self.deadline is not None and datetime.now(UTC) >= self.deadline

    def metadata(self, task: str, start: datetime, end: datetime) -> dict[str, str]:
        return {
            "task": task,
            "interval_start": iso(start),
            "interval_end": iso(end),
            "run_id": self.run_id,
        }


def raw_key(endpoint: str, label: datetime, body_sha: str) -> str:
    """Deterministic bronze key from (endpoint, bucket or snapshot time, content)."""
    return (
        f"bronze/wiki/{endpoint}/date={label.date().isoformat()}/"
        f"{int(label.timestamp())}-{body_sha[:12]}.json.gz"
    )


def store_raw(ctx: Context, endpoint: str, label: datetime, resp: wiki.Response) -> str:
    key = raw_key(endpoint, label, resp.sha256)
    ctx.lake.store.put_if_absent(
        key,
        gzip.compress(resp.body, mtime=0),
        "application/gzip",
        {
            "sha256": resp.sha256,
            "source-url": resp.url,
            "http-status": str(resp.status),
            "fetched-at": iso(resp.fetched_at),
        },
    )
    return key


# ---------------------------------------------------------------- /5m


def manifest(ctx: Context) -> pa.Table:
    """The bucket manifest, read once per process (it is the completeness record)."""
    if "manifest" not in ctx.cache:
        table = ctx.lake.read(BUCKETS, columns=["bucket_start", "item_count", "status"])
        ctx.cache["manifest"] = table if table is not None else BUCKETS_SCHEMA.empty_table()
    return ctx.cache["manifest"]


def first_bucket(ctx: Context) -> datetime | None:
    starts = manifest(ctx).column("bucket_start").to_pylist()
    return min(starts) if starts else None


def missing_buckets(ctx: Context, start: datetime, end: datetime) -> list[datetime]:
    """Closed buckets in [start, end) with no manifest record, newest first."""
    present = set(manifest(ctx).column("bucket_start").to_pylist())
    now = ctx.now()
    closed = [b for b in grid(start, end) if wiki.bucket_is_closed(b, "5m", now)]
    return sorted((b for b in closed if b not in present), reverse=True)


def classify(item_count: int, trailing: list[int]) -> tuple[str, float | None]:
    """'outage' when the count is far below the trailing median; 'ok' otherwise."""
    if len(trailing) < OUTAGE_MIN_HISTORY:
        return "ok", None
    median = float(statistics.median(trailing))
    return ("outage" if item_count < OUTAGE_RATIO * median else "ok"), median


def ingest_5m(ctx: Context, start: datetime, end: datetime) -> dict:
    todo = missing_buckets(ctx, start, end)
    remaining = max(0, len(todo) - ctx.max_buckets)
    todo = todo[: ctx.max_buckets]
    if not todo:
        return {"buckets_fetched": 0, "remaining_missing": 0}

    history = {
        b: n
        for b, n, s in zip(
            manifest(ctx).column("bucket_start").to_pylist(),
            manifest(ctx).column("item_count").to_pylist(),
            manifest(ctx).column("status").to_pylist(),
            strict=True,
        )
        if s == "ok"
    }
    fetched: dict[datetime, tuple[wiki.Response, list[dict]]] = {}
    unpublished = 0
    for bucket in todo:
        resp = ctx.client.bucket("5m", bucket)
        rows = wiki.parse_bucket(resp.json(), bucket)
        if not rows and ctx.now() - (bucket + FIVE_MIN) < UNPUBLISHED_GRACE:
            unpublished += 1  # likely not published yet; retried by the next run
            continue
        fetched[bucket] = (resp, rows)

    price_rows, bucket_rows = [], []
    for bucket in sorted(fetched):  # oldest first, so trailing medians see earlier fetches
        resp, rows = fetched[bucket]
        key = store_raw(ctx, "5m", bucket, resp)
        trailing = [
            n for b, n in history.items() if bucket - TRAILING_BUCKETS * FIVE_MIN <= b < bucket
        ]
        status, median = classify(len(rows), trailing)
        if status == "ok":
            history[bucket] = len(rows)
        regime = regime_at("measurement", bucket)
        for r in rows:
            price_rows.append(
                {
                    **r,
                    "bucket_start": bucket,
                    "bucket_date": bucket.date(),
                    "regime_id": regime,
                    "raw_sha256": resp.sha256,
                }
            )
        bucket_rows.append(
            {
                "bucket_start": bucket,
                "bucket_date": bucket.date(),
                "item_count": len(rows),
                "trailing_median_items": median,
                "status": status,
                "regime_id": regime,
                "raw_key": key,
                "raw_sha256": resp.sha256,
                "fetched_at": resp.fetched_at,
            }
        )
    if not bucket_rows:
        return {"buckets_fetched": 0, "unpublished": unpublished, "remaining_missing": remaining}

    ranges = coalesce_ranges(fetched)
    where_time = " OR ".join(
        f"(bucket_start >= '{sql_ts(a)}' AND bucket_start < '{sql_ts(b)}')" for a, b in ranges
    )
    dates = [d.isoformat() for d in days_touched(fetched)]
    predicate = f"{in_list('bucket_date', dates)} AND ({where_time})"
    meta = ctx.metadata("ingest_5m", start, end)
    price_rows.sort(key=lambda r: (r["bucket_start"], r["item_id"]))
    # Facts first, manifest second: a crash in between leaves the bucket unrecorded, so the
    # next run fetches it again and the replace-where overwrites rather than duplicates.
    ctx.lake.write(
        PRICES,
        pa.Table.from_pylist(price_rows, schema=PRICES_SCHEMA),
        predicate=predicate,
        partition_by=["bucket_date"],
        metadata=meta,
    )
    ctx.lake.write(
        BUCKETS,
        pa.Table.from_pylist(bucket_rows, schema=BUCKETS_SCHEMA),
        predicate=f"({where_time})",
        metadata=meta,
    )
    ctx.cache.pop("manifest", None)
    return {
        "buckets_fetched": len(bucket_rows),
        "rows_written": len(price_rows),
        "outage_buckets": sum(r["status"] == "outage" for r in bucket_rows),
        "unpublished": unpublished,
        "remaining_missing": remaining + unpublished,
        "first_bucket": iso(min(fetched)),
        "last_bucket": iso(max(fetched)),
    }


# ---------------------------------------------------------------- /latest


def _live_only(ctx: Context, end: datetime, window: timedelta) -> str | None:
    lag = ctx.now() - end
    if lag < timedelta(0):
        return f"interval ends in the future ({iso(end)})"
    if lag > window:
        return f"source has no timestamp parameter; {iso(end)} is {lag} in the past"
    return None


def ingest_latest(ctx: Context, start: datetime, end: datetime) -> dict:
    reason = _live_only(ctx, end, LIVE_SOURCE_MAX_AGE)
    if reason:
        return {"status": "skipped", "reason": reason}
    resp = ctx.client.latest()
    rows = wiki.parse_latest(resp.json())
    store_raw(ctx, "latest", end, resp)

    def ts(epoch):
        return None if epoch is None else datetime.fromtimestamp(epoch, tz=end.tzinfo)

    table = pa.Table.from_pylist(
        [
            {
                "snapshot_at": end,
                "snapshot_date": end.date(),
                "fetched_at": resp.fetched_at,
                "item_id": r["item_id"],
                "high": r["high"],
                "high_time": ts(r["high_time"]),
                "low": r["low"],
                "low_time": ts(r["low_time"]),
                "raw_sha256": resp.sha256,
            }
            for r in sorted(rows, key=lambda r: r["item_id"])
        ],
        schema=LATEST_SCHEMA,
    )
    ctx.lake.write(
        LATEST,
        table,
        predicate=f"snapshot_date = '{end.date().isoformat()}' AND snapshot_at = '{sql_ts(end)}'",
        partition_by=["snapshot_date"],
        metadata=ctx.metadata("ingest_latest", start, end),
    )
    return {"rows_written": table.num_rows, "fetched_at": iso(resp.fetched_at)}


# ---------------------------------------------------------------- /mapping (SCD type 2)

ATTRS = ("name", "examine", "members", "lowalch", "highalch", "value", "buy_limit", "icon")


def row_hash(rec: dict) -> str:
    return hashlib.sha256(
        json.dumps([rec[a] for a in ATTRS], separators=(",", ":")).encode()
    ).hexdigest()


def scd2(current: list[dict], snapshot: list[dict], day) -> list[dict]:
    """Apply a mapping snapshot taken on `day` to the item dimension (type 2).

    Changed or removed items close their open version at `day` and changed or new items open
    one from `day`. A second snapshot on the same day replaces that day's versions, so
    re-running the day converges on the latest state instead of stacking versions.
    """
    rows = [dict(r) for r in current if r["valid_from"] != day]
    for r in rows:  # reopen versions that a same-day earlier run closed
        if r["valid_to"] == day:
            r["valid_to"], r["is_current"] = None, True
    open_by_id = {r["item_id"]: r for r in rows if r["valid_to"] is None}
    seen = set()
    for rec in snapshot:
        seen.add(rec["item_id"])
        h = row_hash(rec)
        cur = open_by_id.get(rec["item_id"])
        if cur is not None and cur["row_hash"] == h:
            continue
        if cur is not None:
            cur["valid_to"], cur["is_current"] = day, False
        rows.append(
            {
                **{a: rec[a] for a in ATTRS},
                "item_id": rec["item_id"],
                "row_hash": h,
                "valid_from": day,
                "valid_to": None,
                "is_current": True,
            }
        )
    for item_id, cur in open_by_id.items():
        if item_id not in seen:
            cur["valid_to"], cur["is_current"] = day, False
    return sorted(rows, key=lambda r: (r["item_id"], r["valid_from"]))


def ingest_mapping(ctx: Context, start: datetime, end: datetime) -> dict:
    reason = _live_only(ctx, end, timedelta(days=1))
    if reason:
        return {"status": "skipped", "reason": reason}
    resp = ctx.client.mapping()
    snapshot = wiki.parse_mapping(resp.json())
    store_raw(ctx, "mapping", end, resp)
    existing = ctx.lake.read(ITEMS)
    current = existing.to_pylist() if existing is not None else []
    rows = scd2(current, snapshot, end.date())
    if existing is not None and rows == sorted(
        current, key=lambda r: (r["item_id"], r["valid_from"])
    ):
        return {"status": "success", "changed": 0, "items": len(snapshot)}
    ctx.lake.write(
        ITEMS,
        pa.Table.from_pylist(rows, schema=ITEMS_SCHEMA),
        metadata=ctx.metadata("ingest_mapping", start, end),
    )
    opened = sum(r["valid_from"] == end.date() for r in rows)
    return {"items": len(snapshot), "versions_opened": opened, "rows_written": len(rows)}
