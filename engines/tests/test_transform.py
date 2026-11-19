"""The PySpark transform's rules, on small synthetic inputs (no licensed data needed)."""

import gzip
import json
import random

import duckdb
import polars as pl
import pytest
from ge_engines import inputs, spark_job
from pyspark.sql import functions as F

HOUR = 3600
JAN = inputs.month_bounds("2026-01")[0]  # 2026-01-01 00:00 UTC
SWITCH = inputs.epoch(inputs.parse_utc("2026-09-30T00:00:00Z"))


@pytest.fixture(scope="module")
def spark():
    s = spark_job.session(threads=2, memory="1g", delta=False)
    yield s
    s.stop()


def test_round2_is_duckdbs_rounding_and_sparks_own_round_is_not(spark):
    rng = random.Random(7)
    values = [1.005, 2.675, 0.125, 8.345, 1.255, 0.285, 1234.565, 100.5, 0.0]
    values += [round(rng.uniform(0, 5e6), rng.choice((2, 3, 4))) for _ in range(20_000)]
    values += [rng.uniform(0, 2e9) / rng.randint(1, 50_000) for _ in range(20_000)]
    df = spark.createDataFrame([(v,) for v in values], "x double")
    ours = [r[0] for r in df.select(spark_job.round2(F.col("x"))).collect()]
    native = [r[0] for r in df.select(F.round("x", 2)).collect()]
    con = duckdb.connect()
    con.execute(
        "create table t as select unnest(?)::DOUBLE as x, generate_subscripts(?, 1) i",
        [values, values],
    )
    duck = [r[0] for r in con.execute("select round(x, 2) from t order by i").fetchall()]
    assert ours == duck
    assert native[0] == 1.01 and duck[0] == 1.0  # why round2 exists


def write_bucket(root, endpoint, start, data, *, timestamp=None):
    path = root / "bronze" / "wiki" / endpoint / "date=x" / f"{start}-{'a' * 12}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {"data": data, "timestamp": start if timestamp is None else timestamp}
    path.write_bytes(gzip.compress(json.dumps(body).encode()))
    return path


def test_wiki_rows_parse_as_the_history_job_does(spark, tmp_path):
    full = {"avgHighPrice": 101, "highPriceVolume": 3, "avgLowPrice": 99.5, "lowPriceVolume": 2}
    sparse = {"avgHighPrice": None, "avgLowPrice": 7}  # no volumes: they count as 0
    files = [
        write_bucket(tmp_path, "5m", JAN, {"2": full, "10": sparse}),
        write_bucket(tmp_path, "5m", JAN + 300, {}),  # an empty answer: no rows
    ]
    rows = {r["item_id"]: r.asDict() for r in spark_job.wiki_rows(spark, files).collect()}
    assert rows[2] == {
        "bucket_epoch": JAN,
        "item_id": 2,
        "avg_high_price": 101.0,
        "high_volume": 3,
        "avg_low_price": 99.5,
        "low_volume": 2,
    }
    assert rows[10]["avg_high_price"] is None and rows[10]["high_volume"] == 0
    assert rows[10]["low_volume"] == 0 and len(rows) == 2


def test_a_body_for_another_bucket_is_refused(spark, tmp_path):
    wrong = write_bucket(tmp_path, "5m", JAN, {"2": {}}, timestamp=JAN + 300)
    with pytest.raises(ValueError, match="different bucket"):
        spark_job.wiki_rows(spark, [wrong])


def test_integer_and_double_dump_months_read_alike(spark, tmp_path):
    def month(name, price, volume, ts):
        row = {
            "itemId": [5],
            "timestamp": [JAN // 300],
            "avgLowPrice": [10],
            "avgHighPrice": [12],
            "lowPriceVolume": [4],
            "highPriceVolume": [6],
        }
        types = {
            "itemId": pl.Int32,
            "timestamp": ts,
            "avgLowPrice": price,
            "avgHighPrice": price,
            "lowPriceVolume": volume,
            "highPriceVolume": volume,
        }
        path = tmp_path / name / "data.parquet"
        path.parent.mkdir()
        pl.DataFrame(row, schema=types).write_parquet(path)
        return path

    narrow = month("a", pl.Int32, pl.Int32, pl.Int32)
    wide = month("b", pl.Float64, pl.Int64, pl.Int64)
    a = spark_job.dump_month(spark, narrow).collect()
    b = spark_job.dump_month(spark, wide).collect()
    assert a == b and a[0]["bucket_epoch"] == JAN  # timestamp is a bucket number
    assert spark_job.price_type(spark, narrow) == spark_job.price_type(spark, wide) == "integer"


def test_rollup_weights_by_volume_skips_unpriced_rows_and_labels_hours(spark):
    regimes = inputs.measurement_regimes()
    rows = [
        # hour before the 30 September switch: two buckets, one unpriced on the high side
        (SWITCH - HOUR, 1, 10.0, 1, 20.0, 1, inputs.KAGGLE_SOURCE),
        (SWITCH - HOUR + 300, 1, None, 5, 30.0, 3, inputs.KAGGLE_SOURCE),
        # hour after it, one bucket from each source, and an item that traded at no price
        (SWITCH, 1, 1.0, 1, None, 0, inputs.KAGGLE_SOURCE),
        (SWITCH + 300, 1, 1.03, 2, None, 0, inputs.API_SOURCE),
        (SWITCH, 2, 5.0, 0, None, 0, inputs.KAGGLE_SOURCE),
    ]
    m5 = spark.createDataFrame(
        rows,
        "bucket_epoch bigint, item_id int, avg_high_price double, high_volume bigint, "
        "avg_low_price double, low_volume bigint, source string",
    )
    out = {(r["hour_epoch"], r["item_id"]): r for r in spark_job.rollup(m5, regimes).collect()}
    before, after, unpriced = out[(SWITCH - HOUR, 1)], out[(SWITCH, 1)], out[(SWITCH, 2)]
    assert (before["avg_high_price"], before["high_volume"]) == (10.0, 6)  # all volume summed
    assert before["avg_low_price"] == 27.5 and before["regime_id"] == "measurement-runelite"
    assert after["avg_high_price"] == 1.02 and after["avg_low_price"] is None  # 3.06 / 3
    assert after["source"] == "mixed" and after["regime_id"] == "measurement-jagex"
    assert unpriced["avg_high_price"] is None and unpriced["bucket_month"] == "2026-09"
