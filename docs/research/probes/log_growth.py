# Where the live lake's storage goes (ADR-0019): replays the hourly writers on a local lake
# through ge_intel.lake.Lake (a checkpoint after every commit, as in production), then the
# weekly maintenance, and reports data, Delta log and checkpoint bytes per table.
#
# Row counts and schemas are full size (4,250 mart rows of 38 columns, 4,540 /latest rows,
# about 21,600 5-minute rows an hour); values are synthetic, so the data bytes here are only
# indicative. The reference data bytes are the 19 October 2026 measurement on real responses.
#
#   .venv/bin/python docs/research/probes/log_growth.py <empty dir> [days]
import json, os, random, sys, time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from ge_intel import ingest, ops
from ge_intel.lake import Lake
from ge_intel.objects import LocalStore

root = Path(sys.argv[1]); days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
lake = Lake(root=str(root), store=LocalStore(root))
rnd = random.Random(7)
TS = pa.timestamp("us", tz="UTC")
ITEMS = list(range(2, 2 + 4540))
GOLD_SCHEMA = pa.schema([
    ("snapshot_at", TS), ("snapshot_date", pa.date32()), ("policy_id", pa.string()),
    ("data_grade_version", pa.string()), ("regime_id", pa.string()), ("item_id", pa.int32()),
    ("name", pa.string()), ("members", pa.bool_()), ("buy_limit", pa.int64()),
    ("price_window_start", TS), ("price_window_end", TS), ("instant_buy_vwap", pa.float64()),
    ("instant_sell_vwap", pa.float64()), ("instant_buy_volume_1h", pa.int64()),
    ("instant_sell_volume_1h", pa.int64()), ("instant_sell_volume_entry", pa.int64()),
    ("buy_price", pa.int64()), ("sell_price", pa.int64()), ("tax_exempt", pa.bool_()),
    ("tax_per_item", pa.int64()), ("net_margin", pa.int64()), ("qty_cap_capital", pa.int64()),
    ("qty_cap_volume", pa.int64()), ("feasible_quantity", pa.int64()),
    ("quantity_limited_by", pa.string()), ("net_gp", pa.int64()),
    ("two_sided_hours_24h", pa.int32()), ("price_deviation_pct", pa.float64()),
    ("buckets_ok_24h", pa.int32()), ("outage_buckets_24h", pa.int32()),
    ("last_instant_buy_at", TS), ("last_instant_sell_at", TS), ("latest_fetched_at", TS),
    ("minutes_since_trade", pa.int64()), ("data_grade", pa.string()),
    ("data_grade_reason", pa.string()), ("is_idea", pa.bool_()), ("idea_rank", pa.int64()),
])
assert len(GOLD_SCHEMA) == 38
price = {i: rnd.lognormvariate(7, 3) for i in ITEMS}


def prices_hour(end):
    rows = []
    for k in range(12):
        b = end - timedelta(hours=1) + timedelta(minutes=5 * k)
        for i in rnd.sample(ITEMS, 1800):
            p = price[i] * rnd.uniform(0.98, 1.02)
            rows.append({"bucket_start": b, "bucket_date": b.date(), "item_id": i,
                         "avg_high_price": round(p * 1.01, 2), "high_volume": rnd.randint(1, 5000),
                         "avg_low_price": round(p, 2), "low_volume": rnd.randint(1, 5000),
                         "regime_id": "measurement-jagex", "raw_sha256": f"{b.timestamp():.0f}" * 6})
    rows.sort(key=lambda r: (r["bucket_start"], r["item_id"]))
    return pa.Table.from_pylist(rows, schema=ingest.PRICES_SCHEMA)


def latest_hour(end):
    return pa.Table.from_pylist([
        {"snapshot_at": end, "snapshot_date": end.date(), "fetched_at": end + timedelta(minutes=8),
         "item_id": i, "high": int(price[i] * 1.01), "high_time": end - timedelta(minutes=rnd.randint(0, 600)),
         "low": int(price[i]), "low_time": end - timedelta(minutes=rnd.randint(0, 600)), "raw_sha256": "f" * 64}
        for i in ITEMS], schema=ingest.LATEST_SCHEMA)


def gold_hour(end):
    rows = []
    for rank, i in enumerate(ITEMS[:4250]):
        idea = rnd.random() < 0.2
        grade = rnd.choice("AB") if idea else rnd.choice("ABCC")
        rows.append({"snapshot_at": end, "snapshot_date": end.date(), "policy_id": "casual-v1",
                     "data_grade_version": "data-grade-v2", "regime_id": "measurement-jagex",
                     "item_id": i, "name": f"Item number {i}", "members": i % 3 == 0,
                     "buy_limit": rnd.choice([None, 100, 1000, 10000]),
                     "price_window_start": end - timedelta(hours=1), "price_window_end": end,
                     "instant_buy_vwap": price[i] * 1.01, "instant_sell_vwap": price[i],
                     "instant_buy_volume_1h": rnd.randint(0, 50000), "instant_sell_volume_1h": rnd.randint(0, 50000),
                     "instant_sell_volume_entry": rnd.randint(0, 200000), "buy_price": int(price[i]) + 1,
                     "sell_price": int(price[i] * 1.01), "tax_exempt": False, "tax_per_item": int(price[i] * 0.02),
                     "net_margin": rnd.randint(-1000, 1000), "qty_cap_capital": rnd.randint(1, 10**6),
                     "qty_cap_volume": rnd.randint(0, 20000), "feasible_quantity": rnd.randint(0, 10000),
                     "quantity_limited_by": rnd.choice(["volume", "buy_limit", "capital"]),
                     "net_gp": rnd.randint(1, 10**6) if idea else None, "two_sided_hours_24h": rnd.randint(0, 24),
                     "price_deviation_pct": round(rnd.uniform(0, 20), 1), "buckets_ok_24h": 288, "outage_buckets_24h": 0,
                     "last_instant_buy_at": end - timedelta(minutes=rnd.randint(0, 600)),
                     "last_instant_sell_at": end - timedelta(minutes=rnd.randint(0, 600)),
                     "latest_fetched_at": end + timedelta(minutes=8), "minutes_since_trade": rnd.randint(0, 600),
                     "data_grade": grade, "data_grade_reason": f"both sides traded in {rnd.randint(0, 24)} of the last 24 hours",
                     "is_idea": idea, "idea_rank": rank if idea else None})
    return pa.Table.from_pylist(rows, schema=GOLD_SCHEMA)


def runs_rows(end, n):
    return pa.Table.from_pylist([
        {"run_id": f"sim-{end:%Y%m%d%H}-{n}", "task": t, "interval_start": end - timedelta(hours=1),
         "interval_end": end, "started_at": end, "finished_at": end, "status": "success",
         "detail": json.dumps({"rows_written": 4250, "duration_s": 12.3}), "code_version": "0" * 40,
         "orchestrator": "github-actions"} for t in ("ingest_5m", "ingest_latest", "build_snapshot", "publish", "hourly")
    ], schema=ops.RUNS_SCHEMA)


TABLES = [ingest.PRICES, ingest.BUCKETS, ingest.LATEST, "gold/market_snapshot", ops.TASK_RUNS]


def measure(label):
    out = {}
    for t in TABLES:
        d = root / t
        dt = DeltaTable(str(d))
        aa = pa.table(dt.get_add_actions(flatten=True))
        log = d / "_delta_log"
        js = sum(f.stat().st_size for f in log.glob("*.json"))
        cp = sum(f.stat().st_size for f in log.glob("*.checkpoint.parquet"))
        disk = sum(f.stat().st_size for f in d.rglob("*.parquet") if "_delta_log" not in f.parts)
        out[t] = {"files": aa.num_rows, "active": sum(aa.column("size_bytes").to_pylist()),
                  "disk": disk, "json": js, "checkpoints": cp, "last_checkpoint": max(
                      (f.stat().st_size for f in log.glob("*.checkpoint.parquet")), default=0)}
    tot = {k: sum(v[k] for v in out.values()) for k in ("active", "disk", "json", "checkpoints")}
    print(json.dumps({"at": label, "total_mb": {k: round(v / 1e6, 2) for k, v in tot.items()},
                      "tables": {t: {k: (round(v / 1e6, 3) if k != "files" else v) for k, v in m.items()}
                                 for t, m in out.items()}}))
    sys.stdout.flush()
    return out


def hourly(end):
    p = prices_hour(end)
    lake.write(ingest.PRICES, p, partition_by=["bucket_date"],
               predicate=f"bucket_date = '{(end - timedelta(hours=1)).date()}' AND bucket_start >= "
                         f"'{(end - timedelta(hours=1)):%Y-%m-%d %H:%M:%S}' AND bucket_start < '{end:%Y-%m-%d %H:%M:%S}'")
    b = pa.Table.from_pylist([{"bucket_start": end - timedelta(minutes=60 - 5 * k), "bucket_date": end.date(),
                               "item_count": 1800, "status": "ok", "regime_id": "measurement-jagex"} for k in range(12)],
                             schema=ingest.BUCKETS_SCHEMA)
    lake.write(ingest.BUCKETS, b, predicate=f"bucket_start >= '{(end - timedelta(hours=1)):%Y-%m-%d %H:%M:%S}' AND "
                                            f"bucket_start < '{end:%Y-%m-%d %H:%M:%S}'")
    lake.write(ingest.LATEST, latest_hour(end), partition_by=["snapshot_date"],
               predicate=f"snapshot_date = '{end.date()}' AND snapshot_at = '{end:%Y-%m-%d %H:%M:%S}'")
    lake.write("gold/market_snapshot", gold_hour(end), partition_by=["snapshot_date"],
               predicate=f"snapshot_date = '{end.date()}' AND snapshot_at = '{end:%Y-%m-%d %H:%M:%S}'")
    for n in (1, 2):  # the :06 and :26 runs each record their tasks
        lake.write(ops.TASK_RUNS, runs_rows(end, n), mode="append")


def maintain():
    for t in TABLES:
        dt = DeltaTable(str(root / t))
        m = dt.optimize.compact()
        dt.vacuum(retention_hours=168, dry_run=False, full=True)
        dt.cleanup_metadata()
        dt.create_checkpoint()
        print(json.dumps({"compact": t, "added": m.get("numFilesAdded"), "removed": m.get("numFilesRemoved")}))


start = datetime(2026, 10, 19, 1, tzinfo=UTC)
t0 = time.monotonic()
for d in range(days):
    for h in range(24):
        hourly(start + timedelta(days=d, hours=h))
    measure(f"day {d + 1} ({time.monotonic() - t0:.0f} s)")
maintain()
measure("after compact + VACUUM (7-day retention) + log clean-up")
for t in TABLES:  # a week later: VACUUM takes the files compaction replaced
    DeltaTable(str(root / t)).vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False, full=True)
measure("after VACUUM a week later (simulated: retention 0)")
for t in TABLES:  # and once the log is older than delta's default 30-day log retention
    past = time.time() - 31 * 86400
    for f in (root / t / "_delta_log").iterdir():
        os.utime(f, (past, past))
    DeltaTable(str(root / t)).cleanup_metadata()
measure("after log clean-up with every log file older than 30 days (simulated)")
