"""Retention in the weekly maintenance (ADR-0019), on a local Delta lake built without dbt.

The lake holds a few days of each retained table around the weekly run that ends on Monday
7 December 2026: the mart before, inside and after the 30-day limit (and one day before the
sweep), `/latest` days either side of its limit, and a closed October next to an open November
of 5-minute prices, each October day written as two hourly files.
"""

import json
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import make_context

from ge_intel import history, ingest, maintain, publish, tasks
from ge_intel.build import SNAPSHOT
from ge_intel.ingest import LATEST, PRICES
from ge_intel.lake import content_hash
from ge_intel.maintain import KEEP_ALL, PROPOSED
from ge_intel.ops import RUNS_SCHEMA, TASK_RUNS
from ge_intel.timegrid import FIVE_MIN, HOUR

END = datetime(2026, 12, 7, tzinfo=UTC)  # a Monday: the weekly run for 30 Nov to 7 Dec
START = END - timedelta(days=7)
NOW = END + timedelta(minutes=10)
TS = pa.timestamp("us", tz="UTC")
# The mart's columns that the paper record and the v1 contract read.
MART = pa.schema(
    [
        ("snapshot_at", TS),
        ("snapshot_date", pa.date32()),
        ("regime_id", pa.string()),
        ("item_id", pa.int32()),
        ("name", pa.string()),
        ("members", pa.bool_()),
        ("buy_limit", pa.int64()),
        ("price_window_start", TS),
        ("price_window_end", TS),
        ("instant_buy_volume_1h", pa.int64()),
        ("instant_sell_volume_1h", pa.int64()),
        ("instant_sell_volume_entry", pa.int64()),
        ("buy_price", pa.int64()),
        ("sell_price", pa.int64()),
        ("tax_exempt", pa.bool_()),
        ("tax_per_item", pa.int64()),
        ("net_margin", pa.int64()),
        ("feasible_quantity", pa.int64()),
        ("quantity_limited_by", pa.string()),
        ("net_gp", pa.int64()),
        ("two_sided_hours_24h", pa.int32()),
        ("last_instant_buy_at", TS),
        ("last_instant_sell_at", TS),
        ("minutes_since_trade", pa.int64()),
        ("data_grade", pa.string()),
        ("data_grade_reason", pa.string()),
        ("is_idea", pa.bool_()),
        ("idea_rank", pa.int64()),
    ]
)
# Items 1 and 2 are ideas; 3 is grade C (is_idea false); 4 is grade A with no positive net GP,
# which the mart flags as null, not false.
KINDS = {1: "idea", 2: "idea", 3: "grade_c", 4: "no_margin"}
MART_DAYS = [date(2026, 10, 1), date(2026, 10, 19), date(2026, 11, 6), date(2026, 11, 7)]
LATEST_DAYS = [date(2026, 11, 5), date(2026, 11, 6), date(2026, 11, 7), date(2026, 12, 6)]
PRICE_HOURS = [
    datetime(2026, 10, 30, 10, tzinfo=UTC),
    datetime(2026, 10, 30, 11, tzinfo=UTC),
    datetime(2026, 10, 31, 22, tzinfo=UTC),
    datetime(2026, 10, 31, 23, tzinfo=UTC),
    datetime(2026, 11, 1, 1, tzinfo=UTC),
    datetime(2026, 11, 1, 2, tzinfo=UTC),
]  # hour ends: each holds the two buckets before it


def mart_row(at: datetime, item_id: int) -> dict:
    kind = KINDS[item_id]
    idea = kind == "idea"
    return {
        "snapshot_at": at,
        "snapshot_date": at.date(),
        "regime_id": "measurement-jagex",
        "item_id": item_id,
        "name": f"Item {item_id}",
        "members": False,
        "buy_limit": 100,
        "price_window_start": at - HOUR,
        "price_window_end": at,
        "instant_buy_volume_1h": 500,
        "instant_sell_volume_1h": 400,
        "instant_sell_volume_entry": 1600,
        "buy_price": 100,
        "sell_price": 110 if idea else 101,
        "tax_exempt": False,
        "tax_per_item": 2,
        "net_margin": 8 if idea else -1,
        "feasible_quantity": 100,
        "quantity_limited_by": "buy_limit",
        "net_gp": 800 * (3 - item_id) if idea else None,
        "two_sided_hours_24h": 3 if kind == "grade_c" else 24,
        "last_instant_buy_at": at - FIVE_MIN,
        "last_instant_sell_at": at - FIVE_MIN,
        "minutes_since_trade": 5,
        "data_grade": "C" if kind == "grade_c" else "A",
        "data_grade_reason": "both sides traded in all 24 of the last 24 hours",
        "is_idea": {"idea": True, "grade_c": False, "no_margin": None}[kind],
        "idea_rank": item_id if idea else None,
    }


def write_mart_hour(lake, at: datetime) -> None:
    lake.write(
        SNAPSHOT,
        pa.Table.from_pylist([mart_row(at, i) for i in KINDS], schema=MART),
        predicate=f"snapshot_date = '{at.date()}' AND snapshot_at = '{at:%Y-%m-%d %H:%M:%S}'",
        partition_by=["snapshot_date"],
    )


def write_latest_day(lake, day: date) -> None:
    at = datetime(day.year, day.month, day.day, 12, tzinfo=UTC)
    rows = [
        {"snapshot_at": at, "snapshot_date": day, "fetched_at": at, "item_id": i, "high": 110}
        for i in KINDS
    ]
    lake.write(
        LATEST,
        pa.Table.from_pylist(rows, schema=ingest.LATEST_SCHEMA),
        predicate=f"snapshot_date = '{day}'",
        partition_by=["snapshot_date"],
    )


def write_price_hour(lake, end: datetime) -> None:
    """Two buckets of three items, sorted by bucket then item as `ingest_5m` writes them."""
    rows = [
        {
            "bucket_start": b,
            "bucket_date": b.date(),
            "item_id": i,
            "avg_high_price": 100.0 + i,
            "high_volume": i,
            "avg_low_price": 99.0,
            "low_volume": 1,
            "regime_id": "measurement-jagex",
            "raw_sha256": "a" * 64,
        }
        for b in (end - HOUR, end - HOUR + FIVE_MIN)
        for i in (2, 5, 9)
    ]
    lake.write(
        PRICES,
        pa.Table.from_pylist(rows, schema=ingest.PRICES_SCHEMA),
        predicate=(
            f"bucket_date = '{(end - HOUR).date()}' AND bucket_start >= "
            f"'{end - HOUR:%Y-%m-%d %H:%M:%S}' AND bucket_start < '{end:%Y-%m-%d %H:%M:%S}'"
        ),
        partition_by=["bucket_date"],
    )


@pytest.fixture
def root(lake_root):
    ctx = make_context(lake_root, now=NOW)
    for day in MART_DAYS:
        write_mart_hour(ctx.lake, datetime(day.year, day.month, day.day, 12, tzinfo=UTC))
    write_mart_hour(ctx.lake, END)  # the hour the page publishes
    for day in LATEST_DAYS:
        write_latest_day(ctx.lake, day)
    for end in PRICE_HOURS:
        write_price_hour(ctx.lake, end)
    buckets = [END - HOUR + k * FIVE_MIN for k in range(12)]
    ctx.lake.write(
        ingest.BUCKETS,
        pa.Table.from_pylist(
            [{"bucket_start": b, "bucket_date": b.date(), "status": "ok"} for b in buckets],
            schema=ingest.BUCKETS_SCHEMA,
        ),
    )
    ctx.lake.write(
        TASK_RUNS,
        pa.Table.from_pylist(
            [
                {
                    "run_id": "test",
                    "task": task,
                    "interval_start": END - HOUR,
                    "interval_end": END,
                    "started_at": END + timedelta(minutes=6),
                    "finished_at": END + timedelta(minutes=9),
                    "status": "success",
                    "detail": "{}",
                }
                for task in ("publish", "hourly")
            ],
            schema=RUNS_SCHEMA,
        ),
    )
    return lake_root


def mart_by_day(lake) -> dict[date, set[int]]:
    out: dict[date, set[int]] = {}
    for r in lake.read(SNAPSHOT, columns=["snapshot_date", "item_id"]).to_pylist():
        out.setdefault(r["snapshot_date"], set()).add(r["item_id"])
    return out


def price_files(lake) -> dict[date, list[str]]:
    out: dict[date, list[str]] = {}
    for f in maintain.active_files(lake.table(PRICES), "bucket_date"):
        out.setdefault(f["part"], []).append(f["path"])
    return out


def october(lake):
    return lake.read(
        PRICES, filters=[("bucket_date", "in", [date(2026, 10, 30), date(2026, 10, 31)])]
    )


def test_the_live_setting_keeps_everything_and_reports_the_proposal(root):
    assert maintain.RETENTION == KEEP_ALL  # deletes wait on Sylvester's decision (ADR-0019)
    ctx = make_context(root, now=NOW)
    before = {t: ctx.lake.read(t).num_rows for t in maintain.RETAINED}

    out = maintain.maintain(ctx, START, END)

    assert {t: ctx.lake.read(t).num_rows for t in maintain.RETAINED} == before
    report = out["retention"]
    assert not any(rule["applied"] for rule in report.values())
    assert (report[SNAPSHOT]["from"], report[SNAPSHOT]["before"]) == ("2026-10-03", "2026-11-07")
    assert (report[SNAPSHOT]["days"], report[SNAPSHOT]["rows"]) == (2, 8)
    assert (report[LATEST]["days"], report[LATEST]["rows"]) == (2, 8)
    assert report[PRICES] == {"applied": False, "month": "2026-10", "pending_months": 1} | {
        "days": 2,
        "files": 4,  # retention runs before compaction: two hourly files a day
        "bytes": report[PRICES]["bytes"],
        "rows": 24,
    }
    assert all(t["files"] >= 1 and t["bytes"] > 0 for t in out["tables"].values())
    json.dumps(out)  # recorded in ops/task_runs


def test_the_mart_keeps_every_idea_and_30_days_in_full(root):
    ctx = make_context(root, now=NOW)
    ideas = ctx.lake.read(SNAPSHOT, filters=[("is_idea", "=", True)])

    out = maintain.apply_retention(ctx, PROPOSED, START, END)[SNAPSHOT]

    assert out["applied"] and out["rows_deleted"] == 4 and out["files_rewritten"] == 2
    assert mart_by_day(ctx.lake) == {
        date(2026, 10, 1): {1, 2, 3, 4},  # before this run's sweep
        date(2026, 10, 19): {1, 2},
        date(2026, 11, 6): {1, 2},  # 31 days old at the run
        date(2026, 11, 7): {1, 2, 3, 4},  # 30 days old: kept in full
        date(2026, 12, 7): {1, 2, 3, 4},
    }
    keys = ["snapshot_at", "item_id"]
    kept = ctx.lake.read(SNAPSHOT, filters=[("is_idea", "=", True)])
    assert content_hash(kept, keys) == content_hash(ideas, keys)

    version = ctx.lake.table(SNAPSHOT).version()
    maintain.apply_retention(ctx, PROPOSED, START, END)
    assert ctx.lake.table(SNAPSHOT).version() == version  # nothing left to delete, no commit

    # A wider interval (a manual run after a long pause) reaches the older day too.
    maintain.apply_retention(ctx, PROPOSED, datetime(2026, 9, 1, tzinfo=UTC), END)
    assert mart_by_day(ctx.lake)[date(2026, 10, 1)] == {1, 2}


def test_latest_drops_whole_days_without_rewriting_a_file(root):
    ctx = make_context(root, now=NOW)

    out = maintain.apply_retention(ctx, PROPOSED, START, END)[LATEST]

    assert out["applied"] and out["rows_deleted"] == 8 and out["files_rewritten"] == 0
    days = ctx.lake.read(LATEST, columns=["snapshot_date"]).column("snapshot_date").to_pylist()
    assert set(days) == {date(2026, 11, 7), date(2026, 12, 6)}
    commit = ctx.lake.table(LATEST).history(1)[0]
    assert commit["operation"] == "DELETE"
    assert commit["operationMetrics"]["num_added_files"] == 0


def test_a_closed_month_is_rewritten_in_zstd_sorted_by_item(root):
    ctx = make_context(root, now=NOW)
    keys = ["bucket_start", "item_id"]
    before = content_hash(october(ctx.lake), keys)
    november = price_files(ctx.lake)[date(2026, 11, 1)]
    assert all(len(p) == 2 for p in price_files(ctx.lake).values())  # two hourly files a day

    out = maintain.apply_retention(ctx, PROPOSED, START, END)[PRICES]

    assert out["applied"] and out["month"] == "2026-10" and out["files"] == 4
    assert content_hash(october(ctx.lake), keys) == before  # same rows
    files = price_files(ctx.lake)
    assert files[date(2026, 11, 1)] == november  # November is still open: untouched
    for day in (date(2026, 10, 30), date(2026, 10, 31)):
        assert len(files[day]) == 1 and files[day][0].endswith(".zstd.parquet")
        stored = pq.ParquetFile(root / PRICES / files[day][0])
        assert stored.metadata.row_group(0).column(0).compression == "ZSTD"
        rows = stored.read(columns=["item_id", "bucket_start"]).to_pylist()
        assert rows == sorted(rows, key=lambda r: (r["item_id"], r["bucket_start"]))

    version = ctx.lake.table(PRICES).version()
    assert maintain.apply_retention(ctx, PROPOSED, START, END)[PRICES] == {
        "applied": True,
        "pending_months": 0,
    }
    assert ctx.lake.table(PRICES).version() == version

    maintain.maintain(ctx, START, END)  # compaction leaves a one-file day alone
    assert price_files(ctx.lake)[date(2026, 10, 30)] == files[date(2026, 10, 30)]

    # A late file (a manual backfill) makes the month pending again; the next run tidies it.
    write_price_hour(ctx.lake, datetime(2026, 10, 30, 15, tzinfo=UTC))
    assert maintain.apply_retention(ctx, PROPOSED, START, END)[PRICES]["month"] == "2026-10"
    late = price_files(ctx.lake)[date(2026, 10, 30)]
    assert len(late) == 1 and late[0].endswith(".zstd.parquet")


def test_a_month_waits_until_the_gap_fill_can_no_longer_reach_it(root):
    assert maintain.CLOSED_AFTER >= tasks.LIVE_GAP_WINDOW
    ctx = make_context(root, now=NOW)
    files = maintain.active_files(ctx.lake.table(PRICES), "bucket_date")
    assert list(maintain.pending_months(files, datetime(2026, 11, 7, 23, tzinfo=UTC))) == []
    assert list(maintain.pending_months(files, datetime(2026, 11, 8, tzinfo=UTC))) == ["2026-10"]
    assert list(maintain.pending_months(files, datetime(2026, 12, 8, tzinfo=UTC))) == [
        "2026-10",
        "2026-11",
    ]


def test_retention_keeps_the_published_contract_and_leaves_history_alone(root, monkeypatch):
    assert not set(maintain.RETAINED) & set(history.TABLES)
    monkeypatch.delenv("GE_DEPLOY", raising=False)

    def published(dist):
        monkeypatch.setenv("GE_DIST_DIR", str(dist))
        publish.publish(make_context(root, now=NOW), END - HOUR, END)
        data = dist / "ge" / "data" / "v1"
        return {n: (data / f"{n}.json").read_bytes() for n in ("snapshot", "status")}

    before = published(root.parent / "dist-before")
    monkeypatch.setattr(maintain, "RETENTION", PROPOSED)
    out = maintain.maintain(make_context(root, now=NOW), START, END)
    assert all(rule["applied"] for rule in out["retention"].values())
    assert out["retention"][SNAPSHOT]["rows_deleted"] == 4

    assert published(root.parent / "dist-after") == before
    assert json.loads(before["snapshot"])["counts"]["ideas_published"] == 2
