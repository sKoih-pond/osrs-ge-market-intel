"""One job, three engines: DuckDB, Polars and PySpark (ADR-0011, amended).

The job is the core of the history rebuild: the 68 dump months and the stored `/5m` fills,
rolled up to hourly rows (volume-weighted prices to 0.01 GP, volume sums, regime, source) and
written as Parquet partitioned by month. The 2026-01 repair from `/1h` (744 hourly answers for
one month) is left out, so every engine runs exactly the same work.

    ge-bench prep --history-dir data/history --out data/bench     # fills to one Parquet file
    ge-bench all --history-dir data/history --out data/bench --repeat 3

Each run is a fresh process (an engine's start-up is part of what it costs), engines take turns
so none always runs on a warm page cache, every engine gets the same threads, memory cap and
spill disk, and each run's output is compared with the first DuckDB run's, month by month,
under the parity rules: summary timings count only runs that match, and any run that does not
fails the command. Peak memory is the whole process tree's (the JVM included), sampled every
0.5 s on Linux. Every engine gets the same memory cap, well under the runner's 16 GB: a runner
that runs out of memory loses the whole job, not just the engine.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb

from ge_engines import inputs

ENGINES = ("duckdb", "polars", "pyspark")


def regime_case(column: str) -> str:
    whens = []
    for r in inputs.measurement_regimes():
        cond = f"{column} >= {r.starts}" + (f" and {column} < {r.ends}" if r.ends else "")
        whens.append(f"when {cond} then '{r.regime_id}'")
    return f"case {' '.join(whens)} end"


def _q(path) -> str:
    return str(path).replace("'", "''")


# ---------------------------------------------------------------- prep (not timed)


def prep(history_dir: Path, out: Path) -> dict:
    """The stored `/5m` fills as one Parquet file, so every engine reads the same fills the
    same way (parsing 3,391 JSON files is not the job being compared)."""
    files = [str(p) for p in inputs.raw_files(history_dir, "5m").values()]
    out.mkdir(parents=True, exist_ok=True)
    target = out / "fills_5m.parquet"
    con = duckdb.connect()
    if not files:
        con.execute(
            f"""copy (select null::BIGINT bucket_epoch, null::INTEGER item_id,
                null::DOUBLE avg_high_price, null::BIGINT high_volume,
                null::DOUBLE avg_low_price, null::BIGINT low_volume limit 0)
                to '{_q(target)}' (format parquet)"""
        )
        return {"files": 0, "rows": 0}
    listed = ", ".join(f"'{_q(f)}'" for f in files)
    con.execute(
        f"""
        copy (
          with raw as (
            select "timestamp" as bucket_epoch, unnest(map_entries(data)) as e
            from read_json([{listed}], format = 'auto', compression = 'gzip',
              columns = {{'data': 'MAP(VARCHAR, STRUCT(avgHighPrice DOUBLE,
                           highPriceVolume BIGINT, avgLowPrice DOUBLE, lowPriceVolume BIGINT))',
                         'timestamp': 'BIGINT'}}))
          select bucket_epoch, e.key::INTEGER as item_id,
                 e.value.avgHighPrice as avg_high_price,
                 coalesce(e.value.highPriceVolume, 0) as high_volume,
                 e.value.avgLowPrice as avg_low_price,
                 coalesce(e.value.lowPriceVolume, 0) as low_volume
          from raw
        ) to '{_q(target)}' (format parquet, compression zstd)
        """
    )
    rows = con.execute(f"select count(*) from '{_q(target)}'").fetchone()[0]
    return {"files": len(files), "rows": rows}


# ---------------------------------------------------------------- the job, three ways


def run_duckdb(files: list[Path], fills: Path, out: Path, threads: int, memory_gb: int, tmp: Path):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{memory_gb}GB'")
    con.execute(f"SET temp_directory = '{_q(tmp)}'")
    con.execute("SET preserve_insertion_order = false")
    # The aggregate emits every month mixed, so each thread keeps a write buffer open per month:
    # at the default 524,288 rows that is 68 x 4 buffers of about 40 MB, outside memory_limit,
    # which took a 16 GB runner down. 100,000 rows keeps them near 2 GB.
    con.execute("SET partitioned_write_flush_threshold = 100000")
    listed = ", ".join(f"'{_q(f)}'" for f in files)
    con.execute(
        f"""
        copy (
          with m5 as (
            select "timestamp"::BIGINT * 300 as bucket_epoch, itemId::INTEGER as item_id,
                   avgHighPrice::DOUBLE as avg_high_price, highPriceVolume::BIGINT as high_volume,
                   avgLowPrice::DOUBLE as avg_low_price, lowPriceVolume::BIGINT as low_volume,
                   0::TINYINT as src
            from read_parquet([{listed}], union_by_name = true, hive_partitioning = false)
            union all by name
            select *, 1::TINYINT as src from read_parquet('{_q(fills)}')
          ),
          b as (select *, bucket_epoch - bucket_epoch % 3600 as hour_epoch from m5)
          select to_timestamp(hour_epoch) as bucket_start,
                 strftime(to_timestamp(hour_epoch), '%Y-%m') as bucket_month, item_id,
                 round(sum(avg_high_price * high_volume) filter (where avg_high_price is not null)
                   / nullif(sum(high_volume) filter (where avg_high_price is not null), 0), 2)
                   as avg_high_price,
                 sum(high_volume)::BIGINT as high_volume,
                 round(sum(avg_low_price * low_volume) filter (where avg_low_price is not null)
                   / nullif(sum(low_volume) filter (where avg_low_price is not null), 0), 2)
                   as avg_low_price,
                 sum(low_volume)::BIGINT as low_volume,
                 {regime_case("hour_epoch")} as regime_id,
                 case when min(src) <> max(src) then 'mixed'
                      when min(src) = 1 then '{inputs.API_SOURCE}'
                      else '{inputs.KAGGLE_SOURCE}' end as source
          from b group by hour_epoch, item_id
        ) to '{_q(out)}' (format parquet, partition_by (bucket_month), compression zstd)
        """
    )


def run_polars(files: list[Path], fills: Path, out: Path, threads: int, memory_gb: int, tmp: Path):
    # Polars reads its settings from the environment at import, so they are set first.
    os.environ["POLARS_MAX_THREADS"] = str(threads)
    os.environ["POLARS_OOC_MEMORY_BUDGET_MB"] = str(memory_gb * 1000)
    os.environ["POLARS_OOC_SPILL_DIR"] = str(tmp)
    os.environ["POLARS_OOC_DISK_BUDGET_MB"] = str(40_000)
    import polars as pl

    def typed(frame, code):
        return frame.select(
            pl.col("bucket_epoch").cast(pl.Int64),
            pl.col("item_id").cast(pl.Int32),
            pl.col("avg_high_price").cast(pl.Float64),
            pl.col("high_volume").cast(pl.Int64),
            pl.col("avg_low_price").cast(pl.Float64),
            pl.col("low_volume").cast(pl.Int64),
            pl.lit(code, dtype=pl.Int8).alias("src"),
        )

    dump = [
        typed(
            pl.scan_parquet(f).select(
                (pl.col("timestamp").cast(pl.Int64) * 300).alias("bucket_epoch"),
                pl.col("itemId").alias("item_id"),
                pl.col("avgHighPrice").alias("avg_high_price"),
                pl.col("highPriceVolume").alias("high_volume"),
                pl.col("avgLowPrice").alias("avg_low_price"),
                pl.col("lowPriceVolume").alias("low_volume"),
            ),
            0,
        )
        for f in files
    ]
    m5 = pl.concat([*dump, typed(pl.scan_parquet(fills), 1)])

    def vwap(price: str, volume: str):
        priced = pl.col(price).is_not_null()
        num = pl.when(priced).then(pl.col(price) * pl.col(volume)).sum()
        den = pl.when(priced).then(pl.col(volume)).sum()
        # Not `round(x * 100) / 100`: Polars compiles division by a literal into multiplying
        # by its reciprocal, which moves the last bit of about one price in seven.
        rounded = (num / den).round(2, mode="half_away_from_zero")
        return pl.when(den != 0).then(rounded)

    regime = None
    for r in inputs.measurement_regimes():
        cond = pl.col("hour_epoch") >= r.starts
        if r.ends is not None:
            cond = cond & (pl.col("hour_epoch") < r.ends)
        regime = (pl.when(cond) if regime is None else regime.when(cond)).then(pl.lit(r.regime_id))

    start = pl.from_epoch("hour_epoch", time_unit="s").dt.replace_time_zone("UTC")
    hourly = (
        m5.with_columns(
            (pl.col("bucket_epoch") - pl.col("bucket_epoch") % 3600).alias("hour_epoch")
        )
        .group_by("hour_epoch", "item_id")
        .agg(
            vwap("avg_high_price", "high_volume").alias("avg_high_price"),
            pl.col("high_volume").sum().alias("high_volume"),
            vwap("avg_low_price", "low_volume").alias("avg_low_price"),
            pl.col("low_volume").sum().alias("low_volume"),
            pl.when(pl.col("src").min() != pl.col("src").max())
            .then(pl.lit("mixed"))
            .when(pl.col("src").min() == 1)
            .then(pl.lit(inputs.API_SOURCE))
            .otherwise(pl.lit(inputs.KAGGLE_SOURCE))
            .alias("source"),
        )
        .select(
            start.alias("bucket_start"),
            start.dt.strftime("%Y-%m").alias("bucket_month"),
            "item_id",
            "avg_high_price",
            "high_volume",
            "avg_low_price",
            "low_volume",
            regime.alias("regime_id"),
            "source",
        )
    )
    hourly.sink_parquet(
        pl.PartitionBy(out, key="bucket_month", include_key=False),
        compression="zstd",
        mkdir=True,
    )


def run_pyspark(files: list[Path], fills: Path, out: Path, threads: int, memory_gb: int, tmp: Path):
    from pyspark.sql import functions as F

    from ge_engines import spark_job

    spark = spark_job.session(threads, f"{memory_gb}g", str(tmp), delta=False)
    try:
        m5 = None
        for f in files:
            month = spark_job.dump_month(spark, f)
            m5 = month if m5 is None else m5.unionByName(month)
        m5 = m5.unionByName(
            spark.read.parquet(str(fills)).withColumn("source", F.lit(inputs.API_SOURCE))
        )
        hourly = spark_job.rollup(m5, inputs.measurement_regimes()).drop("hour_epoch")
        hourly.write.mode("overwrite").partitionBy("bucket_month").option(
            "compression", "zstd"
        ).parquet(str(out))
    finally:
        spark.stop()


RUNNERS = {"duckdb": run_duckdb, "polars": run_polars, "pyspark": run_pyspark}


def run_one(
    engine: str, history_dir: Path, fills: Path, out: Path, threads: int, memory_gb: int, tmp: Path
) -> dict:
    files = list(inputs.month_files(history_dir).values())
    shutil.rmtree(out, ignore_errors=True)
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    RUNNERS[engine](files, fills, out, threads, memory_gb, tmp)
    return {"engine": engine, "job_s": round(time.monotonic() - t0, 2), "months": len(files)}


# ---------------------------------------------------------------- the harness


def tree_rss_kb(root_pid: int) -> int | None:
    """Resident memory of a process and all its descendants (Linux /proc), in KiB."""
    proc = Path("/proc")
    if not proc.exists():
        return None
    parent, rss = {}, {}
    for d in proc.iterdir():
        if not d.name.isdigit():
            continue
        try:
            status = (d / "status").read_text()
        except OSError:
            continue
        fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
        parent[int(d.name)] = int(fields.get("PPid", "0").strip())
        rss[int(d.name)] = int(fields.get("VmRSS", "0 kB").split()[0])
    total, todo = 0, [root_pid]
    while todo:
        pid = todo.pop()
        total += rss.get(pid, 0)
        todo += [p for p, pp in parent.items() if pp == pid]
    return total


def contain(pid: int, name: str, limit: str | None) -> Path | None:
    """Put a process, and so every process it starts (the JVM included), in a cgroup of its
    own with a hard memory cap: an engine that runs out of memory is killed alone, not the
    runner with it. Linux cgroup v2 through sudo, as on a GitHub runner; elsewhere nothing."""
    root = Path("/sys/fs/cgroup")
    if not limit or not (root / "cgroup.controllers").exists() or not shutil.which("sudo"):
        return None
    group = root / name
    script = (
        f"mkdir -p {group} && echo {limit} > {group}/memory.max && "
        f"echo 0 > {group}/memory.swap.max && echo {pid} > {group}/cgroup.procs"
    )
    done = subprocess.run(["sudo", "-n", "sh", "-c", script], capture_output=True)
    return group if done.returncode == 0 else None


def oom_kills(group: Path | None) -> int:
    if group is None:
        return 0
    for line in (group / "memory.events").read_text().splitlines():
        key, value = line.split()
        if key == "oom_kill":
            return int(value)
    return 0


def timed(
    cmd: list[str], name: str = "ge-bench", limit: str | None = None
) -> tuple[int, float, int | None, str, bool]:
    """Run `cmd`, contained (`contain`) when `limit` is given; its exit code, wall time, peak
    tree RSS (KiB), last stdout line, and whether the kernel killed it for memory."""
    t0 = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    group = contain(proc.pid, name, limit)
    peak = [None]

    def sample():
        while proc.poll() is None:
            now = tree_rss_kb(proc.pid)
            if now is not None:
                peak[0] = max(peak[0] or 0, now)
            time.sleep(0.5)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    stdout, _ = proc.communicate()
    sampler.join()
    last = stdout.strip().splitlines()[-1] if stdout.strip() else ""
    killed = oom_kills(group) > 0
    return proc.returncode, round(time.monotonic() - t0, 2), peak[0], last, killed


def _view(con, name: str, out: Path) -> None:
    glob = _q(out / "**" / "*.parquet")
    con.execute(
        f"""create or replace view {name} as select * from read_parquet('{glob}',
            hive_partitioning = true, hive_types = {{'bucket_month': 'VARCHAR'}})"""
    )


def compare_outputs(reference: Path, run: Path, memory_gb: int, tmp: Path) -> dict:
    """One run's output against the reference (the first DuckDB run), month by month, with
    the parity rules (ADR-0011, amended): row counts, duplicate keys, keys on one side only,
    volumes and labels must match; prices must match, and two prices on the 0.01 GP grid one
    step apart in whole cents are a rounding flip, reported, never a failure."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET memory_limit = '{memory_gb}GB'")
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{_q(tmp)}'")
    _view(con, "ref", reference)
    _view(con, "run", run)

    def side(s: str) -> str:
        a, b = f"x.avg_{s}_price", f"y.avg_{s}_price"
        ca, cb = f"round({a} * 100)::BIGINT", f"round({b} * 100)::BIGINT"
        grid = f"{a} = {ca}::DOUBLE / 100 and {b} = {cb}::DOUBLE / 100"
        flip = f"({a} <> {b} and abs({ca} - {cb}) = 1 and {grid})"
        error = f"(({a} is null) <> ({b} is null) or ({a} <> {b} and not {flip}))"
        return flip, error

    (hf, he), (lf, le) = side("high"), side("low")
    months = [
        r[0] for r in con.execute("select distinct bucket_month from ref order by 1").fetchall()
    ]
    totals = dict.fromkeys(
        (
            "rows_ref",
            "rows_run",
            "duplicates",
            "missing",
            "extra",
            "volume",
            "labels",
            "rounding_flips",
            "price_errors",
        ),
        0,
    )
    for month in months + ["(other)"]:
        if month == "(other)":
            cond_x, cond_y = "false", "bucket_month not in (select distinct bucket_month from ref)"
        else:
            cond_x = cond_y = f"bucket_month = '{month}'"
        row = con.execute(
            f"""
            with x as (select * from ref where {cond_x}), y as (select * from run where {cond_y}),
                 j as (select x.*, y.item_id as y_item, {he} as e, {le} as e2, {hf} as f,
                              {lf} as f2,
                              x.high_volume <> y.high_volume or x.low_volume <> y.low_volume as v,
                              x.regime_id is distinct from y.regime_id
                                or x.source is distinct from y.source as l
                       from x full outer join y
                         on x.bucket_start = y.bucket_start and x.item_id = y.item_id)
            select (select count(*) from x), (select count(*) from y),
                   (select count(*) - count(distinct (bucket_start, item_id)) from y),
                   count(*) filter (where y_item is null), count(*) filter (where item_id is null),
                   count(*) filter (where item_id is not null and y_item is not null and v),
                   count(*) filter (where item_id is not null and y_item is not null and l),
                   count(*) filter (where item_id is not null and y_item is not null
                                    and (f or f2) and not (e or e2)),
                   count(*) filter (where item_id is not null and y_item is not null and (e or e2))
            from j
            """
        ).fetchone()
        for k, v in zip(totals, row, strict=True):
            totals[k] += v
    hard = ("duplicates", "missing", "extra", "volume", "labels", "price_errors")
    totals["equivalent"] = totals["rows_ref"] == totals["rows_run"] and not any(
        totals[k] for k in hard
    )
    return totals


def summary(results: list[dict]) -> dict:
    """Per engine, over its matching runs only: the first (cold) run and the median of the
    rest (warm), wall time, job time and peak memory."""
    import statistics

    out = {}
    for engine in ENGINES:
        runs = [r for r in results if r["engine"] == engine and r["matches_reference"]]
        if not runs:
            out[engine] = None
            continue
        warm = runs[1:] or runs
        out[engine] = {
            "runs_counted": len(runs),
            "cold_wall_s": runs[0]["wall_s"],
            "warm_wall_s": statistics.median(r["wall_s"] for r in warm),
            "warm_job_s": statistics.median(r["job_s"] for r in warm),
            "startup_s": statistics.median(r["startup_s"] for r in runs),
            "peak_rss_mb": max((r["peak_rss_mb"] or 0) for r in runs) or None,
            "rounding_flips": max(r["rounding_flips"] for r in runs),
        }
    return out


def run_all(args) -> dict:
    out = args.out
    fills = out / "fills_5m.parquet"
    if not fills.exists():
        raise FileNotFoundError(f"run `ge-bench prep` first ({fills})")
    reference = out / "reference"
    shutil.rmtree(reference, ignore_errors=True)
    results = []
    for rep in range(args.repeat):
        order = ENGINES[rep % 3 :] + ENGINES[: rep % 3]  # each engine goes first once
        for engine in order:
            cmd = [
                sys.executable,
                "-m",
                "ge_engines.bench",
                "run",
                "--engine",
                engine,
                "--history-dir",
                str(args.history_dir),
                "--out",
                str(out),
                "--threads",
                str(args.threads),
                "--memory-gb",
                str(args.memory_gb),
            ]
            name = f"ge-bench-{engine}-{rep + 1}"
            code, wall, peak, last, killed = timed(cmd, name, args.memory_max)
            if code != 0:  # a result too: recorded, and the command fails at the end
                results.append(
                    {
                        "engine": engine,
                        "run": rep + 1,
                        "wall_s": wall,
                        "exit_code": code,
                        "out_of_memory": killed,
                        "peak_rss_mb": round(peak / 1024) if peak else None,
                        "matches_reference": False,
                        "rounding_flips": 0,
                    }
                )
                print(json.dumps({"event": "bench_run_failed", **results[-1]}), flush=True)
                continue
            inner = json.loads(last)
            produced = out / "out" / engine
            if engine == "duckdb" and not reference.exists():
                shutil.copytree(produced, reference)  # the first DuckDB output is the reference
            found = (
                compare_outputs(reference, produced, args.memory_gb, out / "tmp" / "compare")
                if reference.exists()
                else {"equivalent": False, "rounding_flips": 0, "rows_ref": None}  # no reference
            )
            results.append(
                {
                    "engine": engine,
                    "run": rep + 1,
                    "wall_s": wall,
                    "job_s": inner["job_s"],
                    "startup_s": round(wall - inner["job_s"], 2),
                    "peak_rss_mb": round(peak / 1024) if peak else None,
                    "matches_reference": found["equivalent"],
                    "rounding_flips": found["rounding_flips"],
                    "comparison": found,
                }
            )
            print(json.dumps({"event": "bench_run", **results[-1]}), flush=True)
    report = {
        "job": "68 dump months + stored /5m fills -> hourly Parquet by month (no /1h repair)",
        "threads": args.threads,
        "memory_gb": args.memory_gb,
        "repeat": args.repeat,
        "versions": versions(),
        "runs": results,
        "summary": summary(results),
        "all_runs_match": all(r["matches_reference"] for r in results),
        "rows": next((r["comparison"]["rows_ref"] for r in results if "comparison" in r), None),
        "memory_max": args.memory_max,
    }
    (out / "results.json").write_text(json.dumps(report, indent=2, sort_keys=True))
    if not report["all_runs_match"]:
        bad = [f"{r['engine']} run {r['run']}" for r in results if not r["matches_reference"]]
        raise RuntimeError(f"runs failed or differ from the reference: {bad} (see results.json)")
    return report


def versions() -> dict:
    import platform

    import polars
    import pyspark

    return {
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "polars": polars.__version__,
        "pyspark": pyspark.__version__,
        "os": platform.platform(),
        "cpus": os.cpu_count(),
        "memory_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ge-bench")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("prep", "run", "all"):
        p = sub.add_parser(name)
        p.add_argument("--history-dir", type=Path, default=Path("data/history"))
        p.add_argument("--out", type=Path, default=Path("data/bench"))
        if name != "prep":
            p.add_argument("--threads", type=int, default=4)
            p.add_argument("--memory-gb", type=int, default=6)
        if name == "run":
            p.add_argument("--engine", choices=ENGINES, required=True)
        if name == "all":
            p.add_argument("--repeat", type=int, default=3)
            p.add_argument(
                "--memory-max",
                help="hard cap per engine process tree, e.g. 12G (Linux cgroup v2 via sudo)",
            )
    args = ap.parse_args(argv)
    if args.cmd == "prep":
        result = prep(args.history_dir, args.out)
    elif args.cmd == "run":
        result = run_one(
            args.engine,
            args.history_dir,
            args.out / "fills_5m.parquet",
            args.out / "out" / args.engine,
            args.threads,
            args.memory_gb,
            args.out / "tmp" / args.engine,
        )
    else:
        result = run_all(args)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
