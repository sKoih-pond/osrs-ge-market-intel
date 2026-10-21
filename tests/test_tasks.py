"""The scheduler's gap-fill window (ADR-0018): years of history in the live manifest, or a long
pause, must never turn the hourly run into a backfill."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
from conftest import FIXTURE_END, FixtureTransport, make_context

from ge_intel import ingest, tasks

GAP_FILL = tasks.REGISTRY["ingest_5m"]


def record_bucket(ctx, start: datetime) -> None:
    row = {"bucket_start": start, "bucket_date": start.date(), "item_count": 1800, "status": "ok"}
    ctx.lake.write(ingest.BUCKETS, pa.Table.from_pylist([row], schema=ingest.BUCKETS_SCHEMA))
    ctx.cache.pop("manifest", None)


def test_history_in_the_live_manifest_cannot_start_a_backfill(lake_root):
    transport = FixtureTransport()
    ctx = make_context(lake_root, transport=transport, max_buckets=96)
    record_bucket(ctx, datetime(2021, 3, 8, 7, 35, tzinfo=UTC))  # the dump's first bucket

    start, end = tasks.interval_for(ctx, GAP_FILL, FIXTURE_END)
    assert (start, end) == (FIXTURE_END - tasks.LIVE_GAP_WINDOW, FIXTURE_END)
    assert len(ingest.missing_buckets(ctx, start, end)) == 7 * 288

    out = ingest.ingest_5m(ctx, start, end)
    asked = [int(u.rsplit("=", 1)[1]) for u in transport.urls]
    assert len(asked) == 96 and min(asked) >= int(start.timestamp())
    assert out["remaining_missing"] == 7 * 288 - 96  # bounded, not 600,000 buckets of history


def test_the_window_keeps_the_young_lake_behaviour(lake_root):
    ctx = make_context(lake_root)
    assert tasks.interval_for(ctx, GAP_FILL, FIXTURE_END)[0] == FIXTURE_END - timedelta(hours=24)
    record_bucket(ctx, FIXTURE_END - timedelta(days=3))
    assert tasks.interval_for(ctx, GAP_FILL, FIXTURE_END)[0] == FIXTURE_END - timedelta(days=3)
