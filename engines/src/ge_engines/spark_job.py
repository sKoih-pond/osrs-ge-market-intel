"""PySpark rebuild of `history/prices_1h` (ADR-0011, amended).

An independent engine over the history job's own inputs (ADR-0018): the pinned dump months,
the stored `/5m` fills and, for repaired months, the stored `/1h` responses. Used for
full-history reprocessing and for a parity check against the DuckDB build. Never in the hourly
path and never on R2: inputs and output are on local disk (ADR-0015).

    ge-spark reprocess --history-dir data/history --out data/history/spark [--months 2026-01,...]

Writes a Delta table at `<out>/history/prices_1h`, partitioned by `bucket_month`, with the
DuckDB build's schema; a run for some months replaces only those months. Prints a JSON report.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ge_engines import inputs
from ge_engines.inputs import API_SOURCE, KAGGLE_SOURCE, Regime

PRICES_1H = "history/prices_1h"
HOUR = 3600
# The stored Wiki body: `{"data": {"<item id>": {...}}, "timestamp": <bucket start>}`. An
# explicit schema, or Spark infers a field per item id.
BUCKET_SCHEMA = T.StructType(
    [
        T.StructField(
            "data",
            T.MapType(
                T.StringType(),
                T.StructType(
                    [
                        T.StructField("avgHighPrice", T.DoubleType()),
                        T.StructField("highPriceVolume", T.LongType()),
                        T.StructField("avgLowPrice", T.DoubleType()),
                        T.StructField("lowPriceVolume", T.LongType()),
                    ]
                ),
            ),
        ),
        T.StructField("timestamp", T.LongType()),
    ]
)
COLUMNS = (
    "bucket_start",
    "bucket_month",
    "item_id",
    "avg_high_price",
    "high_volume",
    "avg_low_price",
    "low_volume",
    "regime_id",
    "source",
)


def session(
    threads: int = 4, memory: str = "10g", local_dir: str | None = None, delta: bool = True
) -> SparkSession:
    """Local mode on one machine. Every setting goes to the JVM at launch (the first
    `getOrCreate` in a process), so the driver memory takes effect. Python workers run this
    interpreter (spike S1: a different `python3` on PATH fails the job)."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master(f"local[{threads}]")
        .appName("ge-intel-history")
        .config("spark.driver.memory", memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(threads * 4))
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.enabled", "false")
    )
    if local_dir:
        builder = builder.config("spark.local.dir", local_dir)
    if not delta:
        return builder.getOrCreate()
    from delta import configure_spark_with_delta_pip

    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------------- inputs


def dump_month(spark: SparkSession, path: Path) -> DataFrame:
    """One dump month as typed 5-minute rows. `timestamp` is a bucket number (epoch seconds /
    300); widths differ by month (int32, int64 or double prices), so each file is read on its
    own and cast."""
    return spark.read.parquet(str(path)).select(
        (F.col("timestamp").cast("bigint") * 300).alias("bucket_epoch"),
        F.col("itemId").cast("int").alias("item_id"),
        F.col("avgHighPrice").cast("double").alias("avg_high_price"),
        F.col("highPriceVolume").cast("bigint").alias("high_volume"),
        F.col("avgLowPrice").cast("double").alias("avg_low_price"),
        F.col("lowPriceVolume").cast("bigint").alias("low_volume"),
        F.lit(KAGGLE_SOURCE).alias("source"),
    )


def price_type(spark: SparkSession, path: Path) -> str:
    """'integer' or 'decimal', read from the values as the history job reads it: a month stored
    as doubles whose prices are all whole coins is an integer month."""
    df = spark.read.parquet(str(path))
    kinds = {f.name: f.dataType for f in df.schema.fields}
    if isinstance(kinds["avgHighPrice"], T.IntegralType) or isinstance(
        kinds["avgLowPrice"], T.IntegralType
    ):
        return "integer"
    fractional = df.where(
        (F.col("avgHighPrice") != F.round("avgHighPrice"))
        | (F.col("avgLowPrice") != F.round("avgLowPrice"))
    ).limit(1)
    return "decimal" if fractional.count() else "integer"


def wiki_rows(spark: SparkSession, files: list[Path]) -> DataFrame:
    """Rows of stored `/5m` or `/1h` responses, as the history job parses them: a missing or
    null volume is 0, a null price stays null. A body for a different bucket than its file name
    says fails the job."""
    if not files:
        return spark.createDataFrame(
            [],
            "bucket_epoch bigint, item_id int, avg_high_price double, "
            "high_volume bigint, avg_low_price double, low_volume bigint",
        )
    raw = (
        spark.read.schema(BUCKET_SCHEMA)
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json([str(f) for f in files])
        .withColumn(
            "name_epoch",
            F.regexp_extract("_metadata.file_path", r"/(\d+)-[0-9a-f]+\.json\.gz$", 1).cast(
                "bigint"
            ),
        )
    )
    wrong = raw.where(F.col("timestamp").isNull() | (F.col("timestamp") != F.col("name_epoch")))
    if wrong.limit(1).count():
        raise ValueError("a stored response holds a different bucket than its file name")
    rows = raw.select(F.col("timestamp").alias("bucket_epoch"), F.explode("data").alias("k", "r"))
    if rows.where(~F.col("k").rlike(r"^[0-9]+$")).limit(1).count():
        raise ValueError("a stored response holds a non-numeric item id")
    return rows.select(
        "bucket_epoch",
        F.col("k").cast("int").alias("item_id"),
        F.col("r.avgHighPrice").alias("avg_high_price"),
        F.coalesce(F.col("r.highPriceVolume"), F.lit(0)).cast("bigint").alias("high_volume"),
        F.col("r.avgLowPrice").alias("avg_low_price"),
        F.coalesce(F.col("r.lowPriceVolume"), F.lit(0)).cast("bigint").alias("low_volume"),
    )


# ---------------------------------------------------------------- the transform


def regime(column: str, regimes: list[Regime]) -> Column:
    """The measurement regime of an epoch-seconds column, from the seed; null outside every
    regime (the run then fails its checks)."""
    expr = None
    for r in regimes:
        cond = F.col(column) >= r.starts
        if r.ends is not None:
            cond = cond & (F.col(column) < r.ends)
        expr = F.when(cond, r.regime_id) if expr is None else expr.when(cond, r.regime_id)
    return expr


def round2(value: Column) -> Column:
    """DuckDB's `round(x, 2)` for doubles, `std::round(x * 100) / 100`. Spark's own
    `round(x, 2)` rounds the shortest decimal string half up, so 1.005 (a double just below
    it) becomes 1.01 there and 1.0 in DuckDB; rounding `x * 100` to a whole number agrees."""
    return F.round(value * 100, 0) / 100


def vwap(price: str, volume: str) -> Column:
    """Volume-weighted price over the rows that have a price, rounded to 0.01 GP; null when
    nothing traded at a price (the denominator is 0 or empty)."""
    priced = F.col(price).isNotNull()
    num = F.sum(F.when(priced, F.col(price) * F.col(volume)))
    den = F.sum(F.when(priced, F.col(volume)))
    return F.when(den != 0, round2(num / den))


def rollup(m5: DataFrame, regimes: list[Regime]) -> DataFrame:
    """5-minute rows to hourly: the history job's `build_month` aggregation, column for column.
    Keeps `hour_epoch` for the repair step."""
    hourly = (
        m5.withColumn("hour_epoch", F.col("bucket_epoch") - F.col("bucket_epoch") % HOUR)
        .groupBy("hour_epoch", "item_id")
        .agg(
            vwap("avg_high_price", "high_volume").alias("avg_high_price"),
            F.sum("high_volume").cast("bigint").alias("high_volume"),
            vwap("avg_low_price", "low_volume").alias("avg_low_price"),
            F.sum("low_volume").cast("bigint").alias("low_volume"),
            F.when(F.min("source") == F.max("source"), F.min("source"))
            .otherwise("mixed")
            .alias("source"),
        )
    )
    return labelled(hourly, regimes)


def labelled(hourly: DataFrame, regimes: list[Regime]) -> DataFrame:
    start = F.timestamp_seconds("hour_epoch")
    return hourly.select(
        "hour_epoch",
        start.alias("bucket_start"),
        F.date_format(start, "yyyy-MM").alias("bucket_month"),
        "item_id",
        "avg_high_price",
        "high_volume",
        "avg_low_price",
        "low_volume",
        regime("hour_epoch", regimes).alias("regime_id"),
        "source",
    )


def repair_hours(month: str, first_bucket: int, live_start: int) -> list[int]:
    """Every hour of a repaired month that history covers."""
    m0, m1 = inputs.month_bounds(month)
    start = max(m0, first_bucket - first_bucket % HOUR)
    return list(range(start, min(m1, live_start), HOUR))


def repair(
    hourly: DataFrame, api: DataFrame, hours: list[int], abs_tol: float, regimes: list[Regime]
) -> tuple[DataFrame, list[int], int]:
    """A month's hourly rows with each hour that disagrees with `/1h` replaced by `/1h`'s rows
    as parsed. An hour disagrees when an item is on one side only, a volume differs, a price is
    null on one side only, or two prices differ by more than the larger of 0.1% and `abs_tol`.
    An hour `/1h` answered empty keeps the dump's rows. Returns the rows, the replaced hours
    and the number of empty answers."""
    api = api.withColumnRenamed("bucket_epoch", "hour_epoch")
    answered = api.select("hour_epoch").distinct()
    m, a = hourly.where(F.col("hour_epoch").isin(hours)).alias("m"), api.alias("a")
    joined = m.join(a, ["hour_epoch", "item_id"], "full_outer").join(
        answered, "hour_epoch", "left_semi"
    )

    def differs(side: str) -> Column:
        mine, theirs = F.col(f"m.avg_{side}_price"), F.col(f"a.avg_{side}_price")
        too_far = F.abs(mine - theirs) > F.greatest(theirs * inputs.REL_TOL, F.lit(abs_tol))
        return (
            (mine.isNull() != theirs.isNull())
            | (mine.isNotNull() & theirs.isNotNull() & too_far)
            | ~F.col(f"m.{side}_volume").eqNullSafe(F.col(f"a.{side}_volume"))
        )

    bad = joined.where(differs("high") | differs("low")).select("hour_epoch").distinct()
    replaced = sorted(r[0] for r in bad.collect())
    empty = len(hours) - answered.count()
    if not replaced:
        return hourly, replaced, empty
    kept = hourly.where(~F.col("hour_epoch").isin(replaced))
    rows = labelled(
        api.where(F.col("hour_epoch").isin(replaced)).withColumn("source", F.lit(API_SOURCE)),
        regimes,
    )
    return kept.unionByName(rows), replaced, empty


# ---------------------------------------------------------------- the job


def write(df: DataFrame, path: str, months: list[str], exists: bool) -> None:
    """One commit replacing exactly `months` (the whole table when it is new), one file per
    month sorted by item and hour, as the DuckDB build writes it."""
    writer = (
        df.select(*COLUMNS)
        .repartition("bucket_month")
        .sortWithinPartitions("item_id", "bucket_start")
        .write.format("delta")
        .partitionBy("bucket_month")
        .mode("overwrite")
    )
    if exists:
        listed = ", ".join(f"'{m}'" for m in months)
        writer = writer.option("replaceWhere", f"bucket_month IN ({listed})")
    writer.save(path)


def validate(spark: SparkSession, path: str, months: list[str], live_start: int) -> dict:
    """Per month written: rows, and the checks the DuckDB build makes before it writes (every
    hour inside its month and before live data, every hour in a regime). Fails the job."""
    listed = ", ".join(f"'{m}'" for m in months)
    stats = (
        spark.read.format("delta")
        .load(path)
        .where(f"bucket_month IN ({listed})")
        .groupBy("bucket_month")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.min(F.unix_seconds("bucket_start")).alias("first"),
            F.max(F.unix_seconds("bucket_start")).alias("last"),
            F.count_if(F.col("regime_id").isNull()).alias("no_regime"),
        )
        .collect()
    )
    out, problems = {}, []
    for r in stats:
        m0, m1 = inputs.month_bounds(r["bucket_month"])
        if r["first"] < m0 or r["last"] >= min(m1, live_start) or r["no_regime"]:
            problems.append(r.asDict())
        out[r["bucket_month"]] = r["rows"]
    missing = sorted(set(months) - set(out))
    if problems or missing:
        raise ValueError(f"rebuild checks failed: {problems or ''} {missing or ''}")
    return out


def reprocess(
    spark: SparkSession,
    history_dir: Path,
    out: Path,
    months: list[str] | None = None,
    *,
    live_start: datetime = inputs.LIVE_START,
    window_5m_start: datetime = inputs.WINDOW_5M_START,
    repair_months: tuple[str, ...] = inputs.REPAIR_FROM_1H,
    seed: Path = inputs.SEED,
) -> dict:
    files = inputs.month_files(history_dir)
    if not files:
        raise FileNotFoundError(f"no dump months under {history_dir / inputs.DUMP_DIR}")
    months = sorted(months or files)
    unknown = sorted(set(months) - set(files))
    if unknown:
        raise ValueError(f"no dump file for {unknown}")
    regimes = inputs.measurement_regimes(seed)
    live = inputs.epoch(live_start)
    fills = inputs.raw_files(history_dir, "5m")
    path = str(Path(out) / PRICES_1H)
    from delta.tables import DeltaTable

    exists = DeltaTable.isDeltaTable(spark, path)
    timings: dict[str, float] = {}

    def month_rows(month: str) -> DataFrame:
        m0, m1 = inputs.month_bounds(month)
        own = [p for start, p in fills.items() if m0 <= start < m1]
        dump = dump_month(spark, files[month])
        if not own:
            return dump
        return dump.unionByName(wiki_rows(spark, own).withColumn("source", F.lit(API_SOURCE)))

    plain = [m for m in months if m not in repair_months]
    if plain:
        t0 = time.monotonic()
        m5 = month_rows(plain[0])
        for month in plain[1:]:
            m5 = m5.unionByName(month_rows(month))
        write(rollup(m5, regimes), path, plain, exists)
        exists = True
        timings["rollup_and_write_s"] = round(time.monotonic() - t0, 2)

    repaired = {}
    for month in [m for m in months if m in repair_months]:
        t0 = time.monotonic()
        m0, m1 = inputs.month_bounds(month)
        if max(m0, inputs.epoch(window_5m_start)) < min(m1, live):
            raise ValueError(f"{month}: only hourly-only months can be repaired from /1h")
        first_month = next(iter(files))
        first = (
            dump_month(spark, files[first_month]).agg(F.min("bucket_epoch")).first()[0]
            if month == first_month
            else m0
        )
        hours = repair_hours(month, first, live)
        stored_1h = inputs.raw_files(history_dir, "1h")
        hourly = rollup(month_rows(month), regimes).cache()
        detail = {"hours": len(hours), "replaced": [], "api_empty": 0, "status": "built"}
        if all(h in stored_1h for h in hours):
            tol = inputs.ABS_TOL[price_type(spark, files[month])]
            api = wiki_rows(spark, [stored_1h[h] for h in hours])
            hourly, replaced, empty = repair(hourly, api, hours, tol, regimes)
            detail.update(replaced=[iso(h) for h in replaced], api_empty=empty, status="validated")
        write(hourly, path, [month], exists)
        exists = True
        repaired[month] = detail
        timings[f"repair_{month}_s"] = round(time.monotonic() - t0, 2)

    t0 = time.monotonic()
    rows = validate(spark, path, months, live)
    timings["validate_s"] = round(time.monotonic() - t0, 2)
    return {
        "table": path,
        "months": len(months),
        "rows": sum(rows.values()),
        "rows_by_month": rows,
        "repaired": repaired,
        "timings": timings,
    }


def iso(seconds: int) -> str:
    return datetime.fromtimestamp(seconds, UTC).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ge-spark")
    sub = ap.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("reprocess", help="rebuild history/prices_1h from the inputs on disk")
    run.add_argument("--history-dir", type=Path, default=Path("data/history"))
    run.add_argument("--out", type=Path, default=Path("data/history/spark"))
    run.add_argument("--months", help="comma-separated YYYY-MM (default: every dump month)")
    run.add_argument("--repair-months", help="override the repaired months (tests)")
    run.add_argument("--live-start", type=inputs.parse_utc, help="override (tests)")
    run.add_argument("--window-5m-start", type=inputs.parse_utc, help="override (tests)")
    run.add_argument("--threads", type=int, default=4)
    run.add_argument("--memory", default="10g", help="driver memory, e.g. 10g")
    run.add_argument("--local-dir", help="Spark's spill directory")
    run.add_argument("--report", type=Path, help="also write the JSON report here")
    args = ap.parse_args(argv)

    t0 = time.monotonic()
    spark = session(args.threads, args.memory, args.local_dir)
    started = round(time.monotonic() - t0, 2)
    try:
        report = reprocess(
            spark,
            args.history_dir,
            args.out,
            args.months.split(",") if args.months else None,
            live_start=args.live_start or inputs.LIVE_START,
            window_5m_start=args.window_5m_start or inputs.WINDOW_5M_START,
            repair_months=(
                tuple(filter(None, args.repair_months.split(",")))
                if args.repair_months is not None
                else inputs.REPAIR_FROM_1H
            ),
        )
        jvm = spark.sparkContext._jvm
        report.update(
            engine="pyspark",
            spark=spark.version,
            java=jvm.System.getProperty("java.version"),
            threads=args.threads,
            driver_memory=args.memory,
            session_start_s=started,
            total_s=round(time.monotonic() - t0, 2),
        )
    finally:
        spark.stop()
    text = json.dumps({"event": "spark_reprocess", **report}, sort_keys=True)
    print(text)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
