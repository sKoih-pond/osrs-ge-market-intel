"""The benchmark's three engines produce the same rows from the same synthetic inputs."""

import gzip
import json
import random
from argparse import Namespace

import duckdb
import polars as pl
from ge_engines import bench, inputs

JAN = inputs.month_bounds("2026-01")[0]


def test_polars_round_matches_duckdb_but_dividing_by_a_literal_does_not():
    rng = random.Random(3)
    values = [rng.uniform(0, 2e9) / rng.randint(1, 50_000) for _ in range(20_000)]
    con = duckdb.connect()
    con.execute(
        "create table t as select unnest(?)::DOUBLE as x, generate_subscripts(?, 1) as i",
        [values, values],
    )
    duck = [r[0] for r in con.execute("select round(x, 2) from t order by i").fetchall()]
    frame = pl.LazyFrame({"x": values})
    ours = frame.select(pl.col("x").round(2, mode="half_away_from_zero")).collect()["x"]
    naive = frame.select((pl.col("x") * 100).round(0, mode="half_away_from_zero") / 100)
    assert ours.to_list() == duck
    assert naive.collect()["x"].to_list() != duck  # x / 100 becomes x * 0.01


def history_dir(tmp_path):
    rng = random.Random(5)
    root = tmp_path / "history"
    month = root / inputs.DUMP_DIR / "prices_5m" / "year=2026" / "month=01" / "data.parquet"
    month.parent.mkdir(parents=True)
    rows = [
        {
            "itemId": item,
            "timestamp": (JAN + b * 300) // 300,
            "avgHighPrice": float(rng.randint(1, 5000)) if rng.random() > 0.1 else None,
            "highPriceVolume": rng.randint(0, 50),
            "avgLowPrice": float(rng.randint(1, 5000)),
            "lowPriceVolume": rng.randint(0, 50),
        }
        for b in range(36)
        for item in range(1, 40)
        if b != 13  # a gap the fills cover
    ]
    pl.DataFrame(rows).cast({"itemId": pl.Int32, "timestamp": pl.Int32}).write_parquet(month)
    fill = (
        root / "bronze" / "wiki" / "5m" / "date=2026-01-01" / f"{JAN + 13 * 300}-{'b' * 12}.json.gz"
    )
    fill.parent.mkdir(parents=True)
    data = {str(i): {"avgHighPrice": 10.25 * i, "highPriceVolume": i} for i in range(1, 30)}
    fill.write_bytes(
        gzip.compress(json.dumps({"data": data, "timestamp": JAN + 13 * 300}).encode())
    )
    return root


def test_every_engine_writes_the_same_rows(tmp_path):
    root = history_dir(tmp_path)
    out = tmp_path / "bench"
    assert bench.prep(root, out) == {"files": 1, "rows": 29}
    args = Namespace(history_dir=root, out=out, repeat=1, threads=2, memory_gb=1, memory_max=None)
    report = bench.run_all(args)
    assert report["all_runs_match"] and all(r["matches_reference"] for r in report["runs"])
    assert report["rows"] == 3 * 39 and [r["engine"] for r in report["runs"]] == list(bench.ENGINES)
    assert set(report["summary"]) == set(bench.ENGINES)


def output(path, rows):
    """A benchmark output: Parquet by month, as the engines write it."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(
        "create table t (bucket_start TIMESTAMPTZ, bucket_month VARCHAR, item_id INTEGER, "
        "avg_high_price DOUBLE, high_volume BIGINT, avg_low_price DOUBLE, low_volume BIGINT, "
        "regime_id VARCHAR, source VARCHAR)"
    )
    con.executemany("insert into t values (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"copy t to '{path}' (format parquet, partition_by (bucket_month))")


def test_a_run_that_differs_is_caught_and_a_one_step_flip_is_not(tmp_path):
    row = ["2026-01-01 00:00:00+00", "2026-01", 2, 100.25, 5, 99.5, 3, "r", "s"]
    other = [*row[:2], 3, *row[3:]]
    output(tmp_path / "ref", [row, other])
    flipped = [*row[:3], 100.26, *row[4:]]
    output(tmp_path / "flip", [flipped, other])
    found = bench.compare_outputs(tmp_path / "ref", tmp_path / "flip", 1, tmp_path / "t")
    assert found["equivalent"] and found["rounding_flips"] == 1
    output(tmp_path / "dup", [row, other, other])
    found = bench.compare_outputs(tmp_path / "ref", tmp_path / "dup", 1, tmp_path / "t")
    assert not found["equivalent"] and found["duplicates"] == 1


def test_empty_prices_and_volumes_hide_no_difference(tmp_path):
    row = ["2026-01-01 00:00:00+00", "2026-01", 2, None, 5, 99.5, 3, "r", "s"]  # no high trades
    other = [*row[:2], 3, 100.25, *row[4:]]
    output(tmp_path / "ref", [row, other])
    output(tmp_path / "flip", [[*row[:5], 99.51, *row[6:]], other])
    found = bench.compare_outputs(tmp_path / "ref", tmp_path / "flip", 1, tmp_path / "t")
    assert found["equivalent"] and found["rounding_flips"] == 1
    output(tmp_path / "vol", [[*row[:4], None, *row[5:]], other])
    found = bench.compare_outputs(tmp_path / "ref", tmp_path / "vol", 1, tmp_path / "t")
    assert not found["equivalent"] and found["volume"] == 1
