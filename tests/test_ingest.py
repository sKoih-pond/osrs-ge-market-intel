"""Ingestion: gap-filling, idempotency, crash safety, outage vs no-trade, SCD2."""

from datetime import date, timedelta

import pytest
from conftest import FIXTURE_END, FIXTURE_NOW, Clock, FixtureTransport, make_context

from ge_intel import ingest
from ge_intel.lake import content_hash
from ge_intel.timegrid import grid

DAY_START = FIXTURE_END - timedelta(hours=24)


def manifest_starts(ctx):
    ctx.cache.pop("manifest", None)
    return set(ingest.manifest(ctx).column("bucket_start").to_pylist())


def prices_hash(ctx):
    return content_hash(ctx.lake.read(ingest.PRICES), ["bucket_start", "item_id"])


def test_fills_newest_first_up_to_the_cap_and_reports_the_rest(lake_root):
    ctx = make_context(lake_root, max_buckets=12)
    out = ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=3), FIXTURE_END)
    assert out["buckets_fetched"] == 12 and out["remaining_missing"] == 24
    assert manifest_starts(ctx) == set(grid(FIXTURE_END - timedelta(hours=1), FIXTURE_END))


def test_a_skipped_run_is_backfilled_by_the_next(lake_root):
    # Run for the hour ending E-2h, skip the E-1h run, then run for E.
    ctx = make_context(lake_root, now=FIXTURE_NOW - timedelta(hours=2))
    ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=3), FIXTURE_END - timedelta(hours=2))
    ctx = make_context(lake_root)
    out = ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=3), FIXTURE_END)
    assert out["buckets_fetched"] == 24
    assert manifest_starts(ctx) == set(grid(FIXTURE_END - timedelta(hours=3), FIXTURE_END))


def test_rerun_fetches_nothing_and_changes_nothing(lake_root):
    ctx = make_context(lake_root)
    ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=2), FIXTURE_END)
    before, version = prices_hash(ctx), ctx.lake.table(ingest.PRICES).version()
    transport = FixtureTransport()
    ctx = make_context(lake_root, transport=transport)
    assert (
        ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=2), FIXTURE_END)["buckets_fetched"] == 0
    )
    assert transport.urls == []
    assert prices_hash(ctx) == before and ctx.lake.table(ingest.PRICES).version() == version


def test_deleted_hour_is_refetched_to_identical_content(lake_root):
    ctx = make_context(lake_root)
    ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=2), FIXTURE_END)
    before = prices_hash(ctx)
    hour = f"bucket_start >= '{(FIXTURE_END - timedelta(hours=1)).isoformat()}'"
    ctx.lake.table(ingest.PRICES).delete(hour)
    ctx.lake.table(ingest.BUCKETS).delete(hour)
    ctx = make_context(lake_root)
    assert (
        ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=2), FIXTURE_END)["buckets_fetched"]
        == 12
    )
    assert prices_hash(ctx) == before


def test_crash_between_facts_and_manifest_leaves_no_duplicates(lake_root, monkeypatch):
    clean = make_context(lake_root / "clean")
    ingest.ingest_5m(clean, FIXTURE_END - timedelta(hours=1), FIXTURE_END)

    ctx = make_context(lake_root / "crash")
    real_write = ctx.lake.write

    def dies_on_manifest(name, *args, **kwargs):
        if name == ingest.BUCKETS:
            raise SystemExit("killed after the facts commit")
        return real_write(name, *args, **kwargs)

    monkeypatch.setattr(ctx.lake, "write", dies_on_manifest)
    with pytest.raises(SystemExit):
        ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=1), FIXTURE_END)
    ctx = make_context(lake_root / "crash")
    assert (
        ingest.ingest_5m(ctx, FIXTURE_END - timedelta(hours=1), FIXTURE_END)["buckets_fetched"]
        == 12
    )
    assert prices_hash(ctx) == prices_hash(clean)


def test_empty_bucket_is_an_outage_only_once_it_has_settled(lake_root):
    gap = FIXTURE_END - timedelta(minutes=10)
    ctx = make_context(lake_root, transport=FixtureTransport(drop_buckets=[gap]), max_buckets=288)
    ingest.ingest_5m(ctx, DAY_START, FIXTURE_END)
    statuses = dict(
        zip(
            *[ingest.manifest(ctx).column(c).to_pylist() for c in ("bucket_start", "status")],
            strict=True,
        )
    )
    assert statuses[gap] == "outage"  # settled: the run is 40 minutes after it closed

    fresh = make_context(lake_root / "fresh", transport=FixtureTransport(drop_buckets=[gap]))
    fresh.now = fresh.client.clock = Clock(gap + timedelta(minutes=8))
    out = ingest.ingest_5m(fresh, gap - timedelta(minutes=5), gap + timedelta(minutes=5))
    assert out["unpublished"] == 1 and gap not in manifest_starts(fresh)


@pytest.mark.parametrize(
    ("count", "trailing", "expected"),
    [
        (751, [1848] * 288, "outage"),  # the 14 Oct 2026 update bucket (methodology note)
        (1462, [1848] * 288, "ok"),  # 0.79 of the median: the quietest normal bucket seen
        (0, [1848] * 5, "ok"),  # too little history to judge
    ],
)
def test_outage_rule(count, trailing, expected):
    assert ingest.classify(count, trailing)[0] == expected


def test_item_missing_from_a_bucket_means_no_trade_not_a_zero_row(lake_root):
    ctx = make_context(lake_root)
    ingest.ingest_5m(ctx, FIXTURE_END - timedelta(minutes=5), FIXTURE_END)
    rows = ctx.lake.read(ingest.PRICES).to_pylist()
    payload = FixtureTransport().buckets[str(int((FIXTURE_END - timedelta(minutes=5)).timestamp()))]
    assert {r["item_id"] for r in rows} == {int(k) for k in payload["data"]}
    assert all(r["regime_id"] == "measurement-jagex" for r in rows)


def test_live_only_sources_refuse_old_intervals(lake_root):
    ctx = make_context(lake_root)
    old = FIXTURE_END - timedelta(hours=3)
    assert ingest.ingest_latest(ctx, old - timedelta(hours=1), old)["status"] == "skipped"
    assert (
        ingest.ingest_mapping(ctx, old - timedelta(days=3), old - timedelta(days=2))["status"]
        == "skipped"
    )
    assert ctx.lake.table(ingest.LATEST) is None


def test_bronze_keys_are_deterministic_and_written_once(lake_root):
    ctx = make_context(lake_root)
    ingest.ingest_5m(ctx, FIXTURE_END - timedelta(minutes=5), FIXTURE_END)
    files = list((lake_root / "bronze" / "wiki" / "5m").rglob("*.json.gz"))
    assert len(files) == 1 and files[0].parent.name == "date=2026-10-19"


def item(i, name="Thing", limit=100):
    return {
        "item_id": i,
        "name": name,
        "examine": "x",
        "members": True,
        "lowalch": 1,
        "highalch": 2,
        "value": 3,
        "buy_limit": limit,
        "icon": "x.png",
    }


def test_scd2_opens_closes_and_converges_on_same_day_reruns():
    d1, d2 = date(2026, 10, 19), date(2026, 10, 20)
    v1 = ingest.scd2([], [item(1), item(2)], d1)
    assert [(r["item_id"], r["valid_to"]) for r in v1] == [(1, None), (2, None)]
    v2 = ingest.scd2(v1, [item(1, limit=50)], d2)  # 1 changed, 2 removed
    assert [
        (r["item_id"], r["buy_limit"], r["valid_from"], r["valid_to"], r["is_current"]) for r in v2
    ] == [
        (1, 100, d1, d2, False),
        (1, 50, d2, None, True),
        (2, 100, d1, d2, False),
    ]
    again = ingest.scd2(v2, [item(1, limit=60), item(2)], d2)  # second snapshot the same day
    assert [(r["item_id"], r["buy_limit"], r["valid_to"]) for r in again] == [
        (1, 100, d2),
        (1, 60, None),
        (2, 100, None),
    ]
    assert ingest.scd2(v1, [item(1), item(2)], d2) == v1  # no change, no new versions
