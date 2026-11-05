"""Parity between builds of `history/prices_1h` (ADR-0011, amended).

Two builds of the same months are compared month by month, in layers, all computed by one
engine (DuckDB) over both, so the hasher is never the thing under test:
- counts: rows per month, and against R2 the row counts its Delta log records and the
  `history/months` manifest;
- a keyed comparison on (hour, item): rows on one side only, volumes, regime and source must
  match exactly; prices must match exactly, and a price within one rounding step (0.01 GP) is
  counted as a rounding flip rather than an error, so a flip is reported, never hidden;
- per-month checksums (volume sums, prices in whole hundredths, an order-independent content
  hash) that any later run can compare without either table.

The builds are read from Delta tables (local, or R2's own files copied to the runner once:
about one GET per file); nothing here writes to R2.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
from deltalake import DeltaTable

from ge_intel import history
from ge_intel.lake import Lake
from ge_intel.maintain import active_files

KEY = "bucket_start, item_id"
# One rounding step of a price held to 0.01 GP, with float slack.
ROUNDING_FLIP = 0.0100001
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


def connect(tmp: Path | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    if tmp is not None:
        tmp.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = '{str(tmp).replace(chr(39), chr(39) * 2)}'")
    return con


def register_delta(con, name: str, uri: str, storage_options: dict | None = None) -> None:
    """A view over a Delta table's live files, partition column included."""
    dataset = DeltaTable(uri, storage_options=storage_options).to_pyarrow_dataset()
    con.register(f"{name}_ds", dataset)
    con.execute(f"create or replace temp view {name} as select {', '.join(COLUMNS)} from {name}_ds")


def register_files(con, name: str, root: Path) -> None:
    """A view over a Delta table's data files copied as they are (partition values in the
    folder names, as Delta writes them)."""
    glob = str(root / "**" / "*.parquet").replace("'", "''")
    con.execute(
        f"""create or replace temp view {name} as select {", ".join(COLUMNS)}
        from read_parquet('{glob}', hive_partitioning = true,
                          hive_types = {{'bucket_month': 'VARCHAR'}})"""
    )


def checksums(con, view: str) -> dict[str, dict]:
    """Per month: rows, volume sums, prices in whole hundredths, and an order-independent
    content hash (a sum of row hashes, so a duplicated row changes it)."""
    rows = con.execute(
        f"""
        select bucket_month, count(*) as rows,
               sum(high_volume)::HUGEINT as high_volume, sum(low_volume)::HUGEINT as low_volume,
               count(avg_high_price) as high_priced, count(avg_low_price) as low_priced,
               sum(round(avg_high_price * 100))::HUGEINT as high_cents,
               sum(round(avg_low_price * 100))::HUGEINT as low_cents,
               sum(hash(bucket_start, item_id, avg_high_price, high_volume, avg_low_price,
                        low_volume, regime_id, source))::HUGEINT as content_hash
        from {view} group by bucket_month order by bucket_month
        """
    ).fetchall()
    names = [d[0] for d in con.description]
    return {
        r[0]: {
            k: (str(v) if k.endswith(("hash", "cents", "volume")) else v)
            for k, v in zip(names[1:], r[1:], strict=True)
        }
        for r in rows
    }


def compare_month(con, a: str, b: str, month: str) -> dict:
    """The keyed comparison of one month of two builds."""
    out = con.execute(
        f"""
        with x as (select * from {a} where bucket_month = $m),
             y as (select * from {b} where bucket_month = $m),
             j as (select x.*, y.item_id as y_item, y.high_volume as y_hv, y.low_volume as y_lv,
                          y.avg_high_price as y_hp, y.avg_low_price as y_lp,
                          y.regime_id as y_regime, y.source as y_source, y.bucket_start as y_start
                   from x full outer join y
                     on x.bucket_start = y.bucket_start and x.item_id = y.item_id),
             d as (select *, greatest(coalesce(abs(avg_high_price - y_hp), 0),
                                      coalesce(abs(avg_low_price - y_lp), 0)) as diff,
                          ((avg_high_price is null) <> (y_hp is null)
                           or (avg_low_price is null) <> (y_lp is null)) as null_differs
                   from j)
        select count(*) filter (where y_item is null) as only_a,
               count(*) filter (where item_id is null) as only_b,
               count(*) filter (where item_id is not null and y_item is not null
                                and (high_volume <> y_hv or low_volume <> y_lv)) as volume,
               count(*) filter (where item_id is not null and y_item is not null
                                and (regime_id is distinct from y_regime
                                     or source is distinct from y_source)) as labels,
               count(*) filter (where item_id is not null and y_item is not null
                                and not null_differs and diff > 0
                                and diff <= {ROUNDING_FLIP}) as rounding_flips,
               count(*) filter (where item_id is not null and y_item is not null
                                and (null_differs or diff > {ROUNDING_FLIP})) as price_errors,
               coalesce(max(diff) filter (where item_id is not null and y_item is not null), 0)
                 as max_price_diff
        from d
        """,
        {"m": month},
    ).fetchone()
    names = [d[0] for d in con.description]
    found = dict(zip(names, out, strict=True))
    # Where the flips are (keys and sources, no prices): enough to explain them.
    found["flip_samples"] = (
        [
            {"bucket_start": str(r[0]), "item_id": r[1], "source": r[2]}
            for r in con.execute(
                f"""
            select x.bucket_start, x.item_id, x.source
            from {a} x join {b} y on x.bucket_start = y.bucket_start and x.item_id = y.item_id
            where x.bucket_month = $m and greatest(
                    coalesce(abs(x.avg_high_price - y.avg_high_price), 0),
                    coalesce(abs(x.avg_low_price - y.avg_low_price), 0))
                  between 1e-12 and {ROUNDING_FLIP}
            order by 1, 2 limit 10
            """,
                {"m": month},
            ).fetchall()
        ]
        if found["rounding_flips"]
        else []
    )
    found["exact"] = not any(
        found[k] for k in ("only_a", "only_b", "volume", "labels", "rounding_flips", "price_errors")
    )
    found["equivalent"] = not any(
        found[k] for k in ("only_a", "only_b", "volume", "labels", "price_errors")
    )
    return found


def compare(con, a: str, b: str, months: list[str] | None = None) -> dict:
    """Both builds month by month: counts, checksums and the keyed comparison."""
    sums_a, sums_b = checksums(con, a), checksums(con, b)
    months = sorted(months or set(sums_a) | set(sums_b))
    by_month = {}
    for month in months:
        found = compare_month(con, a, b, month)
        found["rows_a"] = sums_a.get(month, {}).get("rows", 0)
        found["rows_b"] = sums_b.get(month, {}).get("rows", 0)
        found["checksums_equal"] = sums_a.get(month) == sums_b.get(month)
        by_month[month] = found
    total = {
        k: sum(m[k] for m in by_month.values())
        for k in (
            "rows_a",
            "rows_b",
            "only_a",
            "only_b",
            "volume",
            "labels",
            "rounding_flips",
            "price_errors",
        )
    }
    return {
        "months": len(months),
        "exact": all(m["exact"] for m in by_month.values()),
        "equivalent": all(m["equivalent"] for m in by_month.values()),
        "checksums_equal": all(m["checksums_equal"] for m in by_month.values()),
        "totals": total,
        "max_price_diff": max((m["max_price_diff"] for m in by_month.values()), default=0),
        "months_not_exact": [m for m, v in by_month.items() if not v["exact"]],
        "by_month": by_month,
        "checksums": sums_a,
    }


# ---------------------------------------------------------------- R2 (read only)


def lake_counts(lake: Lake) -> dict[str, int]:
    """Rows per month from `history/prices_1h`'s Delta log: no data read."""
    dt = lake.table(history.PRICES_1H)
    counts: dict[str, int] = {}
    for f in active_files(dt, "bucket_month") if dt is not None else []:
        counts[f["part"]] = counts.get(f["part"], 0) + f["rows"]
    return dict(sorted(counts.items()))


def months_manifest(lake: Lake) -> dict[str, dict]:
    table = lake.read(history.MONTHS, columns=["bucket_month", "hourly_rows", "status"])
    if table is None:
        return {}
    return {r["bucket_month"]: r for r in table.to_pylist()}


def copy_files(lake: Lake, dest: Path) -> dict:
    """Copy `history/prices_1h`'s live files from R2 to `dest` as they are, one GET each (a
    64 MB read size; the files are about 18 MB)."""
    import pyarrow.fs as pafs

    dt = lake.table(history.PRICES_1H)
    opts = lake.storage_options or {}
    s3 = pafs.S3FileSystem(
        access_key=opts["AWS_ACCESS_KEY_ID"],
        secret_key=opts["AWS_SECRET_ACCESS_KEY"],
        endpoint_override=opts["AWS_ENDPOINT_URL"],
        region="auto",
    )
    prefix = lake.uri(history.PRICES_1H).removeprefix("s3://")
    copied, size = 0, 0
    for f in active_files(dt, "bucket_month"):
        target = dest / f["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        pafs.copy_files(
            f"{prefix}/{f['path']}",
            str(target),
            source_filesystem=s3,
            destination_filesystem=pafs.LocalFileSystem(),
            chunk_size=64 * 2**20,
        )
        copied, size = copied + 1, size + f["bytes"]
    return {"files": copied, "bytes": size}


# ---------------------------------------------------------------- the step


def run(
    h: history.History,
    against: Path,
    *,
    r2: Lake | None = None,
    r2_files: bool = False,
    months: list[str] | None = None,
) -> dict:
    """Compare the build at `against` (another engine's `history/prices_1h`, a Delta table)
    with this runner's DuckDB build; with `r2`, also against R2's row counts and manifest, and
    with `r2_files`, against R2's own files. Writes the report beside the history directory."""
    con = connect(h.root / "tmp")
    register_delta(con, "duckdb_build", h.lake.uri(history.PRICES_1H))
    register_delta(con, "other_build", str(Path(against) / history.PRICES_1H))
    report: dict = {"duckdb_vs_other": compare(con, "duckdb_build", "other_build", months)}
    if r2 is not None:
        counts, manifest = lake_counts(r2), months_manifest(r2)
        built = {m: v["rows_a"] for m, v in report["duckdb_vs_other"]["by_month"].items()}
        report["r2_counts"] = {
            "months": len(counts),
            "rows": sum(counts.values()),
            "log_equals_manifest": counts == {m: r["hourly_rows"] for m, r in manifest.items()},
            "log_equals_build": all(counts.get(m) == n for m, n in built.items()),
            "statuses": sorted({r["status"] for r in manifest.values()}),
            "by_month": counts,
        }
        if r2_files:
            copied = copy_files(r2, h.root / "r2_prices_1h")
            register_files(con, "r2_build", h.root / "r2_prices_1h")
            report["r2_copy"] = copied
            report["r2_vs_other"] = compare(con, "r2_build", "other_build", months)
            report["r2_vs_duckdb"] = compare(con, "r2_build", "duckdb_build", months)
    con.close()
    (h.root / "parity.json").write_text(json.dumps(report, indent=2, sort_keys=True, default=str))
    return summary(report)


def agrees(summary: dict) -> bool:
    """Whether the builds agree: equivalent (rounding flips are reported, not failed), and,
    where R2 was read, its counts and files match too."""
    ok = summary["duckdb_vs_other"]["equivalent"]
    counts = summary.get("r2_counts")
    if counts is not None:
        ok = ok and counts["log_equals_manifest"] and counts["log_equals_build"]
    for name in ("r2_vs_other", "r2_vs_duckdb"):
        if name in summary:
            ok = ok and summary[name]["equivalent"]
    return ok


def summary(report: dict) -> dict:
    """The report without per-month detail, for the log line."""
    out = {}
    for name, part in report.items():
        if isinstance(part, dict) and "by_month" in part and "exact" in part:
            out[name] = {k: v for k, v in part.items() if k not in ("by_month", "checksums")}
        elif isinstance(part, dict):
            out[name] = {k: v for k, v in part.items() if k != "by_month"}
        else:
            out[name] = part
    return out
