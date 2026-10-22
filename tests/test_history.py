"""M2 history job (ADR-0018) on a dump-format month built from real Wiki data.

The fixture month (`tests/fixtures/kaggle/`) is the 5-minute fixture written the way the
Kaggle dump writes a month, ending mid-hour at 19 Oct 19:40 UTC and missing three buckets. With
history ending at 22:00 that leaves a 27-bucket seam and three gaps, one of which the API also
returns empty (an outage). `/1h` answers come from real recorded responses.
"""

import gzip
import hashlib
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import FIXTURE_NOW, Clock, FixtureTransport

from ge_intel import cli, history, ingest, kaggle, maintain, tasks, wiki
from ge_intel.lake import Lake, content_hash
from ge_intel.objects import LocalStore
from ge_intel.timegrid import HOUR, grid

KAGGLE = Path(__file__).parent / "fixtures" / "kaggle"
MONTH_FILE = "prices_5m/year=2026/month=10/data.parquet"
LIVE = datetime(2026, 10, 19, 22, tzinfo=UTC)
DUMP_LAST = datetime(2026, 10, 19, 19, 40, tzinfo=UTC)
OUTAGE = datetime(2026, 10, 19, 11, 30, tzinfo=UTC)  # missing from the dump and the API
GAP_HOURS = (3, 11)  # hours of 19 Oct with a bucket missing from the dump
KEYS = {
    history.PRICES_1H: ["bucket_start", "item_id"],
    history.PRICES_5M: ["bucket_start", "item_id"],
    history.BUCKETS_5M: ["bucket_start"],
    history.MONTHS: ["bucket_month"],
}


def fixture_pins():
    return [
        kaggle.Pin(n, (KAGGLE / n).stat().st_size, kaggle.sha256_file(KAGGLE / n))
        for n in ("README.md", MONTH_FILE)
    ]


@pytest.fixture
def h(tmp_path):
    root = tmp_path / "history"
    shutil.copytree(KAGGLE, root / "kaggle" / kaggle.DATASET / "v1")
    client = wiki.WikiClient(
        transport=FixtureTransport(drop_buckets=[OUTAGE]),
        clock=Clock(FIXTURE_NOW),
        min_interval_s=0,
    )
    return history.History(root, client=client, live_start=LIVE, pins=fixture_pins())


def requested(h, endpoint):
    return [int(u.rsplit("=", 1)[1]) for u in h.client.transport.urls if f"/{endpoint}?" in u]


def hashes(lake):
    return {name: content_hash(lake.read(name), keys) for name, keys in KEYS.items()}


def local_lake(path):
    return Lake(root=str(path), store=LocalStore(path))


def write_month(path, row, price, volume):
    schema = pa.schema(
        [
            ("itemId", pa.int32()),
            ("timestamp", pa.int32()),
            ("avgLowPrice", price),
            ("avgHighPrice", price),
            ("lowPriceVolume", volume),
            ("highPriceVolume", volume),
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([row], schema=schema), path)


def test_dump_timestamps_are_bucket_numbers_and_widths_are_unified(tmp_path):
    old = tmp_path / "prices_5m/year=2021/month=03/data.parquet"
    new = tmp_path / "prices_5m/year=2026/month=10/data.parquet"
    base = {"itemId": 2, "lowPriceVolume": 3, "highPriceVolume": 4}
    prices = {"avgLowPrice": 150, "avgHighPrice": 152}
    write_month(old, {**base, **prices, "timestamp": 5_383_963}, pa.int32(), pa.int32())
    newer = {"avgLowPrice": 1.5, "avgHighPrice": None, "timestamp": 5_973_000}
    write_month(new, {**base, **newer}, pa.float64(), pa.int64())

    con = duckdb.connect()
    rel = con.sql(f"select * from ({history.dump_sql([old, new])}) order by bucket_epoch")
    assert [str(t) for t in rel.types] == [
        "BIGINT",
        "INTEGER",
        "DOUBLE",
        "BIGINT",
        "DOUBLE",
        "BIGINT",
    ]
    first, second = rel.fetchall()
    # 5,383,963 is a bucket number: x 300 is the first bucket of Wiki data, 8 Mar 2021 07:35.
    assert history.utc(first[0]) == datetime(2021, 3, 8, 7, 35, tzinfo=UTC)
    assert first[2] == 152.0 and second[2] is None and second[4] == 1.5
    assert history.price_type(con, old) == "integer"
    assert history.price_type(con, new) == "decimal"
    assert history.check_month(con, "2021-03", old, LIVE)["rows"] == 1

    seconds = tmp_path / "seconds.parquet"  # what the file would hold if the README were right
    write_month(seconds, {**base, **prices, "timestamp": 1_615_188_900}, pa.int32(), pa.int32())
    with pytest.raises(wiki.ContractError, match="outside_month"):
        history.check_month(con, "2021-03", seconds, LIVE)


def test_build_dry_run_reports_the_seam_and_gaps_and_writes_nothing(h):
    report = history.build(h, dry_run=True)
    assert report["dump_last_bucket"] == "2026-10-19T19:40:00Z"
    assert (report["expected_buckets"], report["dump_buckets"]) == (288, 258)
    assert report["missing_buckets"] == 30 and report["missing_after_dump_end"] == 27
    assert not (h.root / "lake").exists() and h.client.transport.urls == []


def test_hourly_rollup_of_real_5m_matches_the_real_1h(h):
    history.build(h)
    hourly = h.lake.read(history.PRICES_1H).to_pylist()
    tol, checked = history.ABS_TOL["decimal"], 0
    for hour in grid(LIVE - timedelta(hours=24), DUMP_LAST - HOUR, HOUR):
        if hour.hour in GAP_HOURS:
            continue
        payload = h.client.transport.hours[str(int(hour.timestamp()))]
        rollup = [r for r in hourly if r["bucket_start"] == hour]
        result = history.compare_hour(rollup, wiki.parse_bucket(payload, hour), tol)
        assert result["ok"], (hour, result)
        assert result["max_abs_price_diff"] <= 0.0101  # one rounding step, never more
        checked += 1
    assert checked == 19
    assert {r["regime_id"] for r in hourly} == {"measurement-jagex"}


def test_check_1h_validates_the_month_with_two_requests(h):
    history.build(h)
    assert history.check_1h(h, dry_run=True)["requests"] == 2
    assert h.client.transport.urls == []
    out = history.check_1h(h)
    assert out["build"]["months_validated"] == 1 and len(requested(h, "1h")) == 2
    month = h.lake.read(history.MONTHS).to_pylist()[0]
    hours = [datetime.fromisoformat(x) for x in json.loads(month["check_hours"])]
    assert month["status"] == "validated" and len(hours) == 2
    assert all(x.hour not in GAP_HOURS and x + HOUR <= DUMP_LAST for x in hours)
    assert history.check_1h(h)["requests"] == 0  # already on disk


def test_a_mismatch_quarantines_the_month_and_keeps_it_out_of_r2(h, tmp_path):
    history.build(h)
    hours = json.loads(h.lake.read(history.MONTHS).to_pylist()[0]["check_hours"])
    payload = h.client.transport.hours[str(int(datetime.fromisoformat(hours[0]).timestamp()))]
    item = next(iter(payload["data"].values()))
    item["highPriceVolume"] = item.get("highPriceVolume", 0) + 1
    built = history.check_1h(h)["build"]
    assert built["months_quarantined"] == 1
    detail = json.loads(h.lake.read(history.MONTHS).to_pylist()[0]["check_detail"])
    bad = [d for d in detail if not d["ok"]]
    assert len(bad) == 1 and bad[0]["volume_mismatches"]
    assert built["quarantined"] == {"2026-10": detail}  # the evidence, in the run's log

    target = local_lake(tmp_path / "r2")
    report = history.upload(h, target)
    assert report["months_held_back"] == ["2026-10"] and report["months_uploaded"] == 0
    assert target.table(history.PRICES_1H) is None and target.table(history.PRICES_5M) is None
    assert target.read(history.MONTHS).to_pylist()[0]["status"] == "quarantined"


def test_fill_fetches_the_seam_first_then_gaps_and_records_outages(h):
    history.build(h)
    dry = history.fill(h, dry_run=True)
    assert (dry["missing_buckets"], dry["seam_buckets"], dry["gap_buckets"]) == (30, 27, 3)
    assert dry["seam"] == ["2026-10-19T19:45:00Z", "2026-10-19T22:00:00Z"]
    out = history.fill(h)
    fetched = requested(h, "5m")
    assert len(fetched) == 30 and fetched == sorted(fetched, reverse=True)
    assert history.utc(fetched[0]) == LIVE - timedelta(minutes=5)
    assert out["build"]["missing_buckets"] == 0 and out["build"]["outage_buckets"] == 1

    manifest = {r["bucket_start"]: r for r in h.lake.read(history.BUCKETS_5M).to_pylist()}
    last = LIVE - timedelta(minutes=5)
    assert set(manifest) == set(grid(LIVE - timedelta(hours=24), LIVE))
    assert manifest[OUTAGE]["status"] == "outage" and manifest[OUTAGE]["item_count"] == 0
    assert manifest[last]["source"] == history.SOURCE_API
    assert manifest[last]["fetched_at"] == FIXTURE_NOW
    assert manifest[DUMP_LAST]["source"] == kaggle.SOURCE and manifest[DUMP_LAST]["raw_key"] is None

    hourly = h.lake.read(history.PRICES_1H).to_pylist()

    def sources(hour):
        return {r["source"] for r in hourly if r["bucket_start"] == hour}

    assert "mixed" in sources(datetime(2026, 10, 19, 19, tzinfo=UTC))
    assert sources(datetime(2026, 10, 19, 21, tzinfo=UTC)) == {history.SOURCE_API}
    recorded = h.client.transport.buckets
    outage = str(int(OUTAGE.timestamp()))
    expected = sum(len(p["data"]) for ts, p in recorded.items() if ts != outage)
    assert h.lake.read(history.PRICES_5M).num_rows == expected
    assert history.fill(h)["requests"] == 0


def test_rebuild_from_disk_reproduces_every_table(h):
    history.build(h)
    history.check_1h(h)
    history.fill(h)
    before, sent = hashes(h.lake), len(h.client.transport.urls)
    shutil.rmtree(h.root / "lake")
    history.build(h)
    assert hashes(h.lake) == before and len(h.client.transport.urls) == sent


def test_upload_copies_validated_months_and_meets_live_at_the_seam(h, tmp_path):
    history.build(h)
    history.check_1h(h)
    history.fill(h)
    target = local_lake(tmp_path / "r2")
    dry = history.upload(h, target, dry_run=True, stored_bytes=0)
    assert dry["months_uploaded"] == 1 and dry["commits"] == 4 and dry["bytes"]["total"] > 0
    assert target.table(history.PRICES_1H) is None
    history.upload(h, target)
    history.upload(h, target)  # replace-where per month: a re-run converges
    assert hashes(target) == hashes(h.lake)

    with pytest.raises(history.NotReady, match="5 GB"):
        history.upload(h, target, dry_run=True, stored_bytes=5 * 10**9)
    late = pa.table({"bucket_start": pa.array([LIVE + timedelta(minutes=5)], type=history.TS)})
    target.write(ingest.BUCKETS, late)  # live data that starts after history ends: a hole
    with pytest.raises(history.NotReady, match="live data starts"):
        history.upload(h, target, dry_run=True)


def test_history_and_live_never_share_a_table(h):
    with pytest.raises(ValueError, match="does not write"):
        history.write(h.lake, ingest.BUCKETS, pa.table({"x": [1]}))
    history.build(h)
    history.fill(h)
    assert [p.name for p in (h.root / "lake").iterdir()] == ["history"]
    live_writes = {w for t in tasks.REGISTRY.values() for w in t.writes}
    assert all(name.startswith("history/") for name in history.TABLES)
    assert not live_writes & set(history.TABLES)
    assert not set(maintain.TABLES) & set(history.TABLES)  # weekly VACUUM is a write too
    assert not set(maintain.RETAINED) & set(history.TABLES)  # so are retention deletes
    # The raw responses' table is history's too, but an input: never rebuilt or uploaded.
    assert history.RAW.startswith("history/") and history.RAW not in history.TABLES
    assert history.RAW not in live_writes | set(maintain.TABLES) | set(maintain.RETAINED)


def test_raw_responses_survive_a_throwaway_runner(h, tmp_path):
    history.build(h)
    history.check_1h(h)
    history.fill(h)
    local = history.raw_files(h)
    assert {key.split("/")[2] for key in local} == {"5m", "1h"}
    r2 = local_lake(tmp_path / "r2")

    assert history.push_raw(h, r2, dry_run=True)["new"] == len(local)
    assert r2.table(history.RAW) is None  # a dry run writes nothing
    assert history.push_raw(h, r2)["new"] == len(local)
    version = r2.table(history.RAW).version()
    assert history.push_raw(h, r2) == {
        "on_disk": len(local),
        "in_r2": len(local),
        "new": 0,
        "bytes": 0,
    }
    assert r2.table(history.RAW).version() == version  # nothing new, no commit

    runner = history.History(tmp_path / "runner", pins=h.pins, live_start=LIVE)
    assert history.pull_raw(runner, r2) == {"in_r2": len(local), "restored": len(local)}
    for key, path in local.items():
        copy = runner.root / key
        assert copy.read_bytes() == path.read_bytes()
        meta = path.with_name(path.name + ".meta.json")
        assert copy.with_name(copy.name + ".meta.json").read_text() == meta.read_text()
    assert history.pull_raw(runner, r2)["restored"] == 0
    for endpoint in ("5m", "1h"):  # what build and the step reruns read back
        assert history.stored(runner, endpoint) == history.stored(h, endpoint)

    bad = r2.read(history.RAW).slice(0, 1).to_pylist()[0]
    bad["sha256"] = "0" * 64
    r2.write(
        history.RAW,
        pa.Table.from_pylist([bad], schema=history.RAW_SCHEMA),
        predicate=f"key = '{bad['key']}'",
        partition_by=["endpoint"],
    )
    with pytest.raises(history.RevisionFound, match="recorded SHA-256"):
        history.pull_raw(history.History(tmp_path / "other", pins=h.pins), r2)


def test_the_cli_pushes_and_pulls_raw_responses(h, tmp_path, monkeypatch):
    history.build(h)
    history.fill(h)
    monkeypatch.setenv("GE_LAKE_ROOT", str(tmp_path / "r2"))  # local: no cost guard needed
    monkeypatch.setenv("GE_HISTORY_DIR", str(h.root))
    assert cli.main(["history", "push"]) == 0
    monkeypatch.setenv("GE_HISTORY_DIR", str(tmp_path / "runner"))
    assert cli.main(["history", "pull"]) == 0
    runner = history.History(tmp_path / "runner", pins=h.pins)
    assert history.raw_files(runner).keys() == history.raw_files(h).keys()


def test_plan_reports_a_full_run_without_running_it(h):
    report = history.plan(h)
    assert report["download"]["files_verified"] == 2 and report["download"]["requests"] == 0
    assert report["fill"]["estimate"] and report["fill"]["requests"] == 30
    assert report["check_1h"]["requests"] == 2 and report["totals"]["wiki_requests"] == 32
    history.build(h)
    report = history.plan(h)
    assert (report["check_1h"]["requests"], report["fill"]["requests"]) == (2, 30)
    assert report["upload"]["bytes"]["total"] > 0
    assert h.client.transport.urls == []


def test_two_bodies_for_one_bucket_stop_the_build(h):
    history.build(h)
    history.fill(h, limit=1)
    bucket = LIVE - timedelta(minutes=5)
    other = json.dumps({"data": {}, "timestamp": int(bucket.timestamp())}).encode()
    key = ingest.raw_key("5m", bucket, hashlib.sha256(other).hexdigest())
    h.raw.put_if_absent(key, gzip.compress(other), "application/gzip", {})
    with pytest.raises(history.RevisionFound):
        history.build(h)


def test_history_ends_where_the_live_manifest_starts():
    assert history.LIVE_START == datetime(2026, 10, 18, 23, tzinfo=UTC)
    assert history.WINDOW_5M_START + timedelta(days=90) == datetime(2026, 9, 30, tzinfo=UTC)


def test_check_hours_are_reproducible_and_stable_when_other_hours_change():
    hours = [datetime(2023, 5, 1, tzinfo=UTC) + i * HOUR for i in range(700)]
    picked = history.check_hours("2023-05", "abc", hours)
    assert len(picked) == 2 and picked == history.check_hours("2023-05", "abc", hours[::-1])
    others = [x for x in hours if x not in picked]
    assert history.check_hours("2023-05", "abc", picked + others[100:]) == picked
    assert history.check_hours("2023-05", "other-file", hours) != picked
