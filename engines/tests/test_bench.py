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
    args = Namespace(history_dir=root, out=out, repeat=1, threads=2, memory_gb=1)
    report = bench.run_all(args)
    assert report["outputs_match_duckdb"] == {e: True for e in bench.ENGINES}
    assert report["rows"] == 3 * 39 and [r["engine"] for r in report["runs"]] == list(bench.ENGINES)
