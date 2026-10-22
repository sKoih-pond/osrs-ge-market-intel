"""History (M2): the Kaggle 5-minute dump, Wiki fills and the seam, in tables only this job
writes (ADR-0018).

Inputs are files on local disk that never change once written: the pinned dump
(`kaggle/antonygarand/osrs-ge-5m/v1/`) and every Wiki response this job fetched
(`bronze/wiki/<endpoint>/...`, its request metadata beside it). Every table is rebuilt from
them, so a re-run converges on the same content and a deleted table comes back with the same
hash. The live pipeline never reads or writes `history/`, and this job writes nothing else:
readers join the two at `LIVE_START`.

Steps, each with a dry run that only reports what it would download, request or write:
- `build`: the dump and the fills into `history/*` on the local lake. Local only.
- `check_1h`: two hours per month fetched from the Wiki's `/1h`; any mismatch quarantines the
  month, and a quarantined month is never uploaded.
- `fill`: buckets missing between the dump's first bucket and `LIVE_START`, the seam included,
  fetched from the Wiki's `/5m`, newest first.
- `upload`: validated months to R2.
"""

from __future__ import annotations

import bisect
import gzip
import hashlib
import json
import re
import shutil
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from ge_intel import ingest, kaggle, wiki
from ge_intel.lake import ZSTD, Lake, in_list
from ge_intel.objects import LocalStore
from ge_intel.reference import regimes
from ge_intel.timegrid import FIVE_MIN, HOUR, floor, grid, iso, parse_utc

# The first bucket of `silver/wiki_5m_buckets` (M1 go-live); history ends where live begins.
LIVE_START = datetime(2026, 10, 18, 23, tzinfo=UTC)
# 5-minute rows go to R2 from 90 days before the 30 September 2026 switch (ADR-0005).
WINDOW_5M_START = datetime(2026, 7, 2, tzinfo=UTC)

PRICES_1H = "history/prices_1h"
PRICES_5M = "history/prices_5m"
BUCKETS_5M = "history/buckets_5m"
MONTHS = "history/months"
TABLES = (PRICES_1H, PRICES_5M, BUCKETS_5M, MONTHS)
# The raw Wiki responses this job fetched, kept in R2 so the job can run on a throwaway runner
# (ADR-0020). An input, not a derived table: `build` never writes it and `upload` never copies
# it; `push_raw` adds new responses and `pull_raw` restores them to disk byte for byte.
RAW = "history/raw_wiki"

SOURCE_API = "wiki_api"
CHECK_HOURS = 2
REL_TOL = 0.001
# A checked price passes within the larger of 0.1% and three roundings: the source's 5-minute
# averages (to whole coins in months the dump stores as integers, else to 0.01 GP), this job's
# hourly VWAP (to 0.01 GP, the API's own precision) and the API's hourly average, plus float
# slack. 0.1% alone fails cheap items on rounding alone (docs/plans/M2.md).
ABS_TOL = {"integer": 1.01, "decimal": 0.02}
VWAP_DECIMALS = 2
DUMP_COLUMNS = (
    "itemId",
    "timestamp",
    "avgHighPrice",
    "highPriceVolume",
    "avgLowPrice",
    "lowPriceVolume",
)
MONTH_FILE = re.compile(r"prices_5m/year=(\d{4})/month=(\d{2})/data\.parquet")
MEMORY_LIMIT = "2GB"

# Planning figures for a dry run before the dump is on disk: the dump's README row count and
# listed bytes, and the discovery report's hourly row count and bytes per row (2.2, 2.3).
DUMP_ROWS, DUMP_BYTES = 876_754_705, 4_802_148_436
HOURLY_ROWS_ESTIMATE = 145_000_000
BYTES_PER_ROW = 9.9
SEAM_ESTIMATE = 405  # 17 Oct 13:15 to 18 Oct 23:00 UTC, from the assessment's dump end
# Spike S1: one batched delta-rs commit to R2 costs about 10 Class A and up to 50 Class B.
OPS_PER_COMMIT = (10, 50)
# M2 acceptance: storage at most 5 GB (the cost guard halts at 6 GB).
STORAGE_LIMIT_BYTES = 5 * 10**9

TS = pa.timestamp("us", tz="UTC")
PRICES_1H_SCHEMA = pa.schema(
    [
        ("bucket_start", TS),
        ("bucket_month", pa.string()),
        ("item_id", pa.int32()),
        ("avg_high_price", pa.float64()),
        ("high_volume", pa.int64()),
        ("avg_low_price", pa.float64()),
        ("low_volume", pa.int64()),
        ("regime_id", pa.string()),
        ("source", pa.string()),  # the dump, wiki_api, or mixed when an hour has both
    ]
)
PRICES_5M_SCHEMA = pa.schema([*ingest.PRICES_SCHEMA, ("source", pa.string())])
BUCKETS_SCHEMA = pa.schema(
    [
        ("bucket_start", TS),
        ("bucket_month", pa.string()),
        ("item_count", pa.int32()),
        ("trailing_median_items", pa.float64()),
        ("status", pa.string()),  # ok | outage
        ("regime_id", pa.string()),
        ("source", pa.string()),
        ("raw_key", pa.string()),
        ("raw_sha256", pa.string()),
        ("fetched_at", TS),
    ]
)
MONTHS_SCHEMA = pa.schema(
    [
        ("bucket_month", pa.string()),
        ("source_file", pa.string()),
        ("source_sha256", pa.string()),
        ("price_type", pa.string()),  # integer | decimal
        ("dump_rows", pa.int64()),
        ("dump_buckets", pa.int32()),
        ("fill_buckets", pa.int32()),
        ("expected_buckets", pa.int32()),
        ("recorded_buckets", pa.int32()),
        ("outage_buckets", pa.int32()),
        ("hourly_rows", pa.int64()),
        ("check_hours", pa.string()),  # JSON list of hour starts
        ("check_detail", pa.string()),  # JSON, one entry per checked hour
        ("status", pa.string()),  # built | validated | quarantined
    ]
)


RAW_SCHEMA = pa.schema(
    [
        ("key", pa.string()),  # the file's path under the history directory
        ("endpoint", pa.string()),  # 5m | 1h
        ("sha256", pa.string()),  # of the response body
        ("body_gz", pa.binary()),  # the file itself: the body, gzipped with mtime 0
        ("meta", pa.string()),  # the request metadata file beside it, verbatim
    ]
)


class NotReady(RuntimeError):
    """A step's inputs are missing (for example, the dump is not fully downloaded)."""


class RevisionFound(RuntimeError):
    """Two different bodies for one bucket; a person decides which stands."""


@dataclass
class History:
    root: Path  # the history directory (default data/history, git-ignored)
    client: wiki.WikiClient | None = None
    live_start: datetime = LIVE_START
    window_5m_start: datetime = WINDOW_5M_START
    pins: list[kaggle.Pin] | None = None
    lake: Lake = field(init=False)
    raw: LocalStore = field(init=False)

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self.lake = Lake(root=str(self.root / "lake"), store=LocalStore(self.root / "lake"))
        self.raw = LocalStore(self.root)
        if self.pins is None:
            self.pins = kaggle.load_pins()

    @property
    def dump(self) -> Path:
        return self.root / "kaggle" / kaggle.DATASET / f"v{kaggle.VERSION}"

    def month_pins(self) -> dict[str, kaggle.Pin]:
        out = {}
        for pin in self.pins:
            m = MONTH_FILE.fullmatch(pin.name)
            if m:
                out[f"{m[1]}-{m[2]}"] = pin
        return dict(sorted(out.items()))


def write(lake: Lake, name: str, data, **kwargs) -> int:
    """`Lake.write` for history tables only: this job owns `history/` and nothing else."""
    if name not in TABLES:
        raise ValueError(f"the history job does not write {name}")
    return lake.write(name, data, writer_properties=ZSTD, **kwargs)


def epoch(moment: datetime) -> int:
    return int(moment.timestamp())


def utc(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, UTC)


def month_of(moment: datetime) -> str:
    return moment.strftime("%Y-%m")


def month_bounds(month: str) -> tuple[datetime, datetime]:
    year, mon = map(int, month.split("-"))
    return datetime(year, mon, 1, tzinfo=UTC), datetime(
        year + mon // 12, mon % 12 + 1, 1, tzinfo=UTC
    )


def _q(path: Path | str) -> str:
    return str(path).replace("'", "''")


def connect(h: History) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    (h.root / "tmp").mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{_q(h.root / 'tmp')}'")  # large months spill to disk
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")  # leaves room on an 8 GB laptop
    return con


def regime_sql(column: str, kind: str = "measurement") -> str:
    """A SQL CASE giving the regime id for an epoch-seconds column, from the regimes seed."""
    whens = []
    for r in regimes():
        if r.kind == kind:
            cond = f"{column} >= {epoch(r.starts_at)}"
            if r.ends_at is not None:
                cond += f" and {column} < {epoch(r.ends_at)}"
            whens.append(f"when {cond} then '{r.regime_id}'")
    return f"case {' '.join(whens)} end"


def dump_sql(paths: list[Path]) -> str:
    """Dump files as typed 5-minute rows. `timestamp` is a bucket number (epoch seconds / 300,
    not the epoch seconds its README says); widths differ by month, so columns are unified by
    name and cast; the hive path is ignored (its `month` key reads as text in some files and a
    number in others) and the month comes from the timestamp instead."""
    files = ", ".join(f"'{_q(p)}'" for p in paths)
    return f"""
        select "timestamp"::BIGINT * 300 as bucket_epoch,
               itemId::INTEGER as item_id,
               avgHighPrice::DOUBLE as avg_high_price,
               highPriceVolume::BIGINT as high_volume,
               avgLowPrice::DOUBLE as avg_low_price,
               lowPriceVolume::BIGINT as low_volume
        from read_parquet([{files}], union_by_name = true, hive_partitioning = false)
    """


def price_type(con, path: Path) -> str:
    """'integer' or 'decimal' prices; refuses a file without the Wiki's field names."""
    types = dict(con.execute("select name, type from parquet_schema(?)", [str(path)]).fetchall())
    missing = [c for c in DUMP_COLUMNS if c not in types]
    if missing:
        raise wiki.ContractError(f"{path}: missing columns {missing}")
    return (
        "integer"
        if {types["avgHighPrice"], types["avgLowPrice"]} & {"INT32", "INT64"}
        else "decimal"
    )


CHECKS = (
    "outside_month",
    "after_live_start",
    "bad_item",
    "null_volume",
    "negative",
    "both_sides_empty",
    "duplicates",
)


def check_month(con, month: str, path: Path, live_start: datetime) -> dict:
    """Shape checks on one dump month; any failure stops the build (fail loudly)."""
    m0, m1 = month_bounds(month)
    values = con.execute(
        f"""
        with d as ({dump_sql([path])})
        select count(*), count(distinct bucket_epoch),
               count(distinct (item_id, bucket_epoch // 3600)),
               count(*) filter (where bucket_epoch < {epoch(m0)} or bucket_epoch >= {epoch(m1)}),
               count(*) filter (where bucket_epoch >= {epoch(live_start)}),
               count(*) filter (where item_id is null or item_id <= 0),
               count(*) filter (where high_volume is null or low_volume is null),
               count(*) filter (where avg_high_price < 0 or avg_low_price < 0
                                or high_volume < 0 or low_volume < 0),
               count(*) filter (where avg_high_price is null and avg_low_price is null),
               count(*) - count(distinct (item_id, bucket_epoch))
        from d
        """
    ).fetchone()
    stats = dict(zip(("rows", "buckets", "hourly_rows", *CHECKS), values, strict=True))
    problems = {k: stats[k] for k in CHECKS if stats[k]}
    if problems:
        hint = " (is `timestamp` still a bucket number?)" if "outside_month" in problems else ""
        raise wiki.ContractError(f"dump month {month}: {problems}{hint}")
    return stats


def conform(data, schema: pa.Schema):
    """Columns in the schema's order and types (delta-rs keeps a table's first schema)."""
    return data.select(schema.names).cast(schema)


# ---------------------------------------------------------------- inputs on disk


@dataclass(frozen=True)
class Stored:
    start: datetime
    key: str
    sha256: str
    fetched_at: datetime | None
    payload: dict


def stored(h: History, endpoint: str) -> dict[datetime, Stored]:
    """Every Wiki response this job kept for `endpoint`, by bucket start."""
    out: dict[datetime, Stored] = {}
    for path in sorted((h.root / "bronze" / "wiki" / endpoint).glob("date=*/*.json.gz")):
        stamp, short = path.name.removesuffix(".json.gz").split("-", 1)
        start = utc(int(stamp))
        body = gzip.decompress(path.read_bytes())
        sha = hashlib.sha256(body).hexdigest()
        if not sha.startswith(short):
            raise RevisionFound(f"{path} does not hash to its name")
        if start in out:
            raise RevisionFound(f"two different {endpoint} bodies for {iso(start)}")
        meta_path = path.with_name(path.name + ".meta.json")
        fetched = (
            json.loads(meta_path.read_text()).get("fetched-at") if meta_path.exists() else None
        )
        out[start] = Stored(
            start,
            str(path.relative_to(h.root)),
            sha,
            parse_utc(fetched) if fetched else None,
            json.loads(body),
        )
    return out


def fetch(h: History, endpoint: str, start: datetime) -> None:
    """One Wiki bucket, kept on local disk with its request metadata (ADR-0015)."""
    if h.client is None:
        raise NotReady("no Wiki client")
    resp = h.client.bucket(endpoint, start)
    wiki.parse_bucket(resp.json(), start)  # never keep a body this pipeline cannot read
    h.raw.put_if_absent(
        ingest.raw_key(endpoint, start, resp.sha256),
        gzip.compress(resp.body, mtime=0),
        "application/gzip",
        {
            "sha256": resp.sha256,
            "source-url": resp.url,
            "http-status": str(resp.status),
            "fetched-at": iso(resp.fetched_at),
        },
    )


def raw_files(h: History) -> dict[str, Path]:
    """Every Wiki response this job kept on disk, by its path under the history directory."""
    found = sorted((h.root / "bronze" / "wiki").glob("*/date=*/*.json.gz"))
    return {str(path.relative_to(h.root)): path for path in found}


def push_raw(h: History, target: Lake, *, dry_run: bool = False) -> dict:
    """Copy the responses on disk that `target` does not hold yet into `history/raw_wiki`, in
    one commit (replace-where on their keys, so a re-run converges). Nothing new, no commit."""
    remote = target.read(RAW, columns=["key"])
    held = set(remote.column("key").to_pylist()) if remote is not None else set()
    local = raw_files(h)
    rows = []
    for key in sorted(set(local) - held):
        path = local[key]
        body_gz = path.read_bytes()
        meta = path.with_name(path.name + ".meta.json")
        rows.append(
            {
                "key": key,
                "endpoint": path.parts[-3],
                "sha256": hashlib.sha256(gzip.decompress(body_gz)).hexdigest(),
                "body_gz": body_gz,
                "meta": meta.read_text() if meta.exists() else None,
            }
        )
    report = {
        "on_disk": len(local),
        "in_r2": len(held),
        "new": len(rows),
        "bytes": sum(len(r["body_gz"]) for r in rows),
    }
    if dry_run or not rows:
        return report
    target.write(
        RAW,
        pa.Table.from_pylist(rows, schema=RAW_SCHEMA),
        predicate=in_list("key", [r["key"] for r in rows]),
        partition_by=["endpoint"],
        metadata={"task": "history_push_raw"},
    )
    return report


def pull_raw(h: History, source: Lake) -> dict:
    """Restore the responses in `history/raw_wiki` to disk. A file already on disk must be
    byte-identical; a missing one is written with its metadata beside it, as `fetch` wrote it."""
    table = source.read(RAW)
    if table is None:
        return {"in_r2": 0, "restored": 0}
    restored = 0
    for row in table.to_pylist():
        key = row["key"]
        if not key.startswith("bronze/wiki/") or ".." in key.split("/"):
            raise ValueError(f"{RAW} holds an unexpected key {key!r}")
        if hashlib.sha256(gzip.decompress(row["body_gz"])).hexdigest() != row["sha256"]:
            raise RevisionFound(f"{key} in {RAW} does not hash to its recorded SHA-256")
        path = h.root / key
        if path.exists():
            if path.read_bytes() != row["body_gz"]:
                raise RevisionFound(f"{path} differs from its copy in {RAW}")
            continue
        meta = json.loads(row["meta"]) if row["meta"] else {}
        h.raw.put_if_absent(key, row["body_gz"], "application/gzip", meta)
        restored += 1
    return {"in_r2": table.num_rows, "restored": restored}


def verified_months(h: History) -> dict[str, tuple[Path, str]]:
    """month -> (path, SHA-256) for every pinned month; refuses if any is missing or differs."""
    out, missing = {}, []
    for month, pin in h.month_pins().items():
        path = kaggle.local_path(h.dump, pin.name)
        if not path.exists():
            missing.append(month)
            continue
        out[month] = (path, kaggle.check(path, pin))
    if missing:
        raise NotReady(f"{len(missing)} of {len(out) + len(missing)} dump months not downloaded")
    return out


# ---------------------------------------------------------------- build


def manifest_rows(
    h: History, dump_counts: dict[int, int], fills: dict[datetime, Stored], shas: dict[str, str]
) -> list[dict]:
    """The bucket manifest from the dump's first bucket to `live_start`: every bucket the dump
    holds or the Wiki returned, classified by the live outage rule (ADR-0016). A bucket that
    neither holds has no row; `fill` fetches it."""
    window: list[int] = []  # item counts of ok buckets in the trailing 24 hours, sorted
    trail: deque[tuple[datetime, int]] = deque()
    rows = []
    for bucket in grid(utc(min(dump_counts)), h.live_start):
        fill = fills.get(bucket)
        if epoch(bucket) in dump_counts:
            if fill is not None:
                raise RevisionFound(f"{iso(bucket)} is in the dump and was also fetched")
            count, source, key = dump_counts[epoch(bucket)], kaggle.SOURCE, None
            sha, fetched = shas[month_of(bucket)], None
        elif fill is not None:
            count, source, key = len(fill.payload["data"]), SOURCE_API, fill.key
            sha, fetched = fill.sha256, fill.fetched_at
        else:
            continue
        while trail and trail[0][0] < bucket - ingest.TRAILING_BUCKETS * FIVE_MIN:
            window.pop(bisect.bisect_left(window, trail.popleft()[1]))
        status, median = ingest.classify(count, window)
        if status == "ok":
            bisect.insort(window, count)
            trail.append((bucket, count))
        rows.append(
            {
                "bucket_start": bucket,
                "bucket_month": month_of(bucket),
                "item_count": count,
                "trailing_median_items": median,
                "status": status,
                "regime_id": None,  # from the seed, in SQL
                "source": source,
                "raw_key": key,
                "raw_sha256": sha,
                "fetched_at": fetched,
            }
        )
    return rows


def complete_dump_hours(manifest: list[dict]) -> dict[str, list[datetime]]:
    """Per month, the hours whose 12 buckets are all in the dump. Depends on the dump alone, so
    fills and outage flags (which fills can shift) never change which hours qualify."""
    count: dict[datetime, int] = {}
    for r in manifest:
        if r["source"] == kaggle.SOURCE:
            hour = floor(r["bucket_start"], HOUR)
            count[hour] = count.get(hour, 0) + 1
    out: dict[str, list[datetime]] = {}
    for hour, n in sorted(count.items()):
        if n == 12:
            out.setdefault(month_of(hour), []).append(hour)
    return out


def check_hours(month: str, sha: str, candidates: list[datetime]) -> list[datetime]:
    """Two of the month's complete dump hours: those whose hash with the source, month and file
    hash is lowest. As good as random, reproducible, and one hour's eligibility never moves
    the pick elsewhere, so a check tests the dump and nothing else."""

    def rank(hour: datetime) -> bytes:
        return hashlib.sha256(f"{kaggle.SOURCE}:{month}:{sha}:{iso(hour)}".encode()).digest()

    return sorted(sorted(candidates, key=rank)[:CHECK_HOURS])


def compare_hour(rollup: list[dict], api: list[dict], abs_tol: float) -> dict:
    """A rolled-up dump hour against the Wiki's `/1h`: the same items, volumes exact, prices
    within the larger of 0.1% and the source's rounding."""
    mine = {r["item_id"]: r for r in rollup}
    theirs = {r["item_id"]: r for r in api}
    volume, price, worst = set(), set(), 0.0
    for i in sorted(mine.keys() & theirs.keys()):
        for side in ("high", "low"):
            if mine[i][f"{side}_volume"] != theirs[i][f"{side}_volume"]:
                volume.add(i)
            a, b = mine[i][f"avg_{side}_price"], theirs[i][f"avg_{side}_price"]
            if (a is None) != (b is None):
                price.add(i)
            elif a is not None:
                worst = max(worst, abs(a - b))
                if abs(a - b) > max(REL_TOL * b, abs_tol):
                    price.add(i)
    only_dump, only_api = mine.keys() - theirs.keys(), theirs.keys() - mine.keys()
    return {
        "ok": not (only_dump or only_api or volume or price),
        "items": len(theirs),
        "only_in_dump": sorted(only_dump)[:5],
        "only_in_api": sorted(only_api)[:5],
        "volume_mismatches": sorted(volume)[:5],
        "price_mismatches": sorted(price)[:5],
        "max_abs_price_diff": round(worst, 6),
    }


FILLS_SCHEMA = pa.schema(
    [
        ("bucket_epoch", pa.int64()),
        ("item_id", pa.int32()),
        ("avg_high_price", pa.float64()),
        ("high_volume", pa.int64()),
        ("avg_low_price", pa.float64()),
        ("low_volume", pa.int64()),
        ("raw_sha256", pa.string()),
    ]
)


def _fills_table(fills: list[Stored]) -> pa.Table:
    rows = [
        {**r, "bucket_epoch": epoch(f.start), "raw_sha256": f.sha256}
        for f in fills
        for r in wiki.parse_bucket(f.payload, f.start)
    ]
    return pa.Table.from_pylist(rows, schema=FILLS_SCHEMA)


def build_month(
    h: History, con, month: str, path: Path, sha: str, fills: list[Stored]
) -> tuple[pa.Table, int]:
    """Write one month's hourly rows, and its 5-minute rows inside the R2 window. Returns the
    hourly rows (for the month's checks) and the number of 5-minute rows written."""
    con.register("fills_5m", _fills_table(fills))
    con.execute(
        f"""
        create or replace temp view m5 as
        select *, '{kaggle.SOURCE}' as source, '{sha}' as raw_sha256 from ({dump_sql([path])})
        union all by name
        select *, '{SOURCE_API}' as source from fills_5m
        """
    )
    # VWAPs are rounded to the API's own precision: 18% smaller than full doubles on real data.
    hourly = con.execute(
        f"""
        with b as (select *, bucket_epoch - bucket_epoch % 3600 as hour_epoch from m5)
        select to_timestamp(hour_epoch) as bucket_start, '{month}' as bucket_month, item_id,
               round(sum(avg_high_price * high_volume) filter (where avg_high_price is not null)
                 / nullif(sum(high_volume) filter (where avg_high_price is not null), 0),
                 {VWAP_DECIMALS}) as avg_high_price,
               sum(high_volume)::BIGINT as high_volume,
               round(sum(avg_low_price * low_volume) filter (where avg_low_price is not null)
                 / nullif(sum(low_volume) filter (where avg_low_price is not null), 0),
                 {VWAP_DECIMALS}) as avg_low_price,
               sum(low_volume)::BIGINT as low_volume,
               {regime_sql("hour_epoch")} as regime_id,
               case when min(source) = max(source) then min(source) else 'mixed' end as source
        from b group by hour_epoch, item_id
        order by item_id, bucket_start
        """
    ).to_arrow_table()
    hourly = conform(hourly, PRICES_1H_SCHEMA)
    if hourly.column("regime_id").null_count:
        raise LookupError(f"{month}: hours outside every measurement regime")
    meta = {"task": "history_build", "month": month, "source_sha256": sha}
    write(
        h.lake,
        PRICES_1H,
        hourly,
        predicate=f"bucket_month = '{month}'",
        partition_by=["bucket_month"],
        metadata=meta,
    )
    m0, m1 = month_bounds(month)
    w0 = max(m0, h.window_5m_start)
    if w0 >= m1:
        return hourly, 0
    where = f"bucket_epoch >= {epoch(w0)}"
    rows = con.execute(f"select count(*) from m5 where {where}").fetchone()[0]
    five = con.execute(
        f"""
        select to_timestamp(bucket_epoch) as bucket_start,
               to_timestamp(bucket_epoch)::DATE as bucket_date,
               item_id, avg_high_price, high_volume, avg_low_price, low_volume,
               {regime_sql("bucket_epoch")} as regime_id, raw_sha256, source
        from m5 where {where}
        order by bucket_date, item_id, bucket_start
        """
    ).to_arrow_reader()
    write(
        h.lake,
        PRICES_5M,
        five.cast(PRICES_5M_SCHEMA),
        predicate=f"bucket_date >= '{w0.date()}' AND bucket_date < '{m1.date()}'",
        partition_by=["bucket_date"],
        metadata=meta,
    )
    return hourly, rows


def build(h: History, *, dry_run: bool = False) -> dict:
    """Rebuild every `history/` table on the local lake from the inputs on disk."""
    months = verified_months(h)
    shas = {m: sha for m, (_, sha) in months.items()}
    con = connect(h)
    stats = {m: check_month(con, m, p, h.live_start) for m, (p, _) in months.items()}
    prices = {m: price_type(con, p) for m, (p, _) in months.items()}
    dump_counts = dict(
        con.execute(
            f"select bucket_epoch, count(*) from ({dump_sql([p for p, _ in months.values()])}) "
            "group by bucket_epoch"
        ).fetchall()
    )
    fills = stored(h, "5m")
    first, last = utc(min(dump_counts)), utc(max(dump_counts))
    expected = grid(first, h.live_start)
    missing = [b for b in expected if epoch(b) not in dump_counts and b not in fills]
    report = {
        "months": len(months),
        "dump_rows": sum(s["rows"] for s in stats.values()),
        "dump_first_bucket": iso(first),
        "dump_last_bucket": iso(last),
        "expected_buckets": len(expected),
        "dump_buckets": len(dump_counts),
        "fetched_buckets": len(fills),
        "missing_buckets": len(missing),
        "missing_after_dump_end": sum(b > last for b in missing),  # the seam
        "hourly_rows": sum(s["hourly_rows"] for s in stats.values()),
        "price_types": {t: list(prices.values()).count(t) for t in ABS_TOL},
    }
    if dry_run:
        con.close()
        return report

    checks = stored(h, "1h")
    manifest = manifest_rows(h, dump_counts, fills, shas)
    complete = complete_dump_hours(manifest)
    per_month: dict[str, dict[str, int]] = {}
    for r in manifest:
        n = per_month.setdefault(r["bucket_month"], {"recorded": 0, "outage": 0, "fill": 0})
        n["recorded"] += 1
        n["outage"] += r["status"] == "outage"
        n["fill"] += r["source"] == SOURCE_API
    fills_by_month: dict[str, list[Stored]] = {}
    for f in fills.values():
        fills_by_month.setdefault(month_of(f.start), []).append(f)

    month_rows, rows_5m = [], 0
    for month, (path, sha) in months.items():
        hours = check_hours(month, sha, complete.get(month, []))
        hourly, n5 = build_month(h, con, month, path, sha, fills_by_month.get(month, []))
        rows_5m += n5
        detail = []
        for hour in hours:
            if hour in checks:
                rollup = hourly.filter(pc.equal(hourly["bucket_start"], pa.scalar(hour, TS)))
                api = wiki.parse_bucket(checks[hour].payload, hour)
                result = compare_hour(rollup.to_pylist(), api, ABS_TOL[prices[month]])
                detail.append({"hour": iso(hour), "raw_key": checks[hour].key, **result})
        if not hours:
            status = "quarantined"  # no complete dump hour to check
        elif len(detail) < len(hours):
            status = "built"
        else:
            status = "validated" if all(d["ok"] for d in detail) else "quarantined"
        m0, m1 = month_bounds(month)
        n = per_month.get(month, {"recorded": 0, "outage": 0, "fill": 0})
        month_rows.append(
            {
                "bucket_month": month,
                "source_file": h.month_pins()[month].name,
                "source_sha256": sha,
                "price_type": prices[month],
                "dump_rows": stats[month]["rows"],
                "dump_buckets": stats[month]["buckets"],
                "fill_buckets": n["fill"],
                "expected_buckets": len(grid(max(m0, first), min(m1, h.live_start))),
                "recorded_buckets": n["recorded"],
                "outage_buckets": n["outage"],
                "hourly_rows": hourly.num_rows,
                "check_hours": json.dumps([iso(x) for x in hours]),
                "check_detail": json.dumps(detail, sort_keys=True),
                "status": status,
            }
        )
    con.register("manifest", pa.Table.from_pylist(manifest, schema=BUCKETS_SCHEMA))
    buckets = con.execute(
        f"select * replace ({regime_sql('epoch(bucket_start)::BIGINT')} as regime_id) "
        "from manifest order by bucket_start"
    ).to_arrow_table()
    meta = {"task": "history_build"}
    write(
        h.lake,
        BUCKETS_5M,
        conform(buckets, BUCKETS_SCHEMA),
        partition_by=["bucket_month"],
        metadata=meta,
    )
    write(h.lake, MONTHS, pa.Table.from_pylist(month_rows, schema=MONTHS_SCHEMA), metadata=meta)
    con.close()
    for name in TABLES:  # one reader, one writer on the local lake: keep no old versions
        dt = h.lake.table(name)
        if dt is not None:
            dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False, full=True)
    statuses = [r["status"] for r in month_rows]
    return {
        **report,
        "rows_5m_window": rows_5m,
        "outage_buckets": sum(r["status"] == "outage" for r in manifest),
        "months_validated": statuses.count("validated"),
        "months_quarantined": statuses.count("quarantined"),
        "months_unchecked": statuses.count("built"),
        # The evidence a person needs to decide on a quarantined month (plan, section 5.1).
        "quarantined": {
            r["bucket_month"]: json.loads(r["check_detail"])
            for r in month_rows
            if r["status"] == "quarantined"
        },
    }


# ---------------------------------------------------------------- Wiki steps


def built_months(h: History) -> list[dict]:
    table = h.lake.read(MONTHS)
    if table is None or set(table.column("bucket_month").to_pylist()) != set(h.month_pins()):
        raise NotReady("run `ge-intel history build` first: not every dump month is built")
    return table.to_pylist()


def _seconds(h: History, requests: int) -> float:
    return requests * (h.client.min_interval_s if h.client else wiki.MIN_INTERVAL_S)


def check_1h(h: History, *, dry_run: bool = False) -> dict:
    """Fetch each month's two check hours from `/1h` (skipping any already on disk), then
    rebuild, which marks every month validated or quarantined."""
    on_disk = stored(h, "1h")
    hours = [parse_utc(x) for m in built_months(h) for x in json.loads(m["check_hours"])]
    todo = [x for x in hours if x not in on_disk]
    report = {"check_hours": len(hours), "requests": len(todo), "seconds": _seconds(h, len(todo))}
    if dry_run:
        return report
    for hour in todo:
        fetch(h, "1h", hour)
    return {**report, "build": build(h)}


def fill(h: History, *, dry_run: bool = False, limit: int | None = None) -> dict:
    """Fetch missing buckets from `/5m`, newest first so the seam comes first, then rebuild.
    An empty response is kept too: it is the evidence that a gap is a source outage."""
    built_months(h)
    rows = h.lake.read(BUCKETS_5M, columns=["bucket_start", "source"]).to_pylist()
    recorded = {r["bucket_start"] for r in rows}
    dump_end = max(r["bucket_start"] for r in rows if r["source"] == kaggle.SOURCE)
    todo = sorted((b for b in grid(min(recorded), h.live_start) if b not in recorded), reverse=True)
    batch = todo if limit is None else todo[:limit]
    by_month: dict[str, int] = {}
    for b in todo:
        by_month[month_of(b)] = by_month.get(month_of(b), 0) + 1
    report = {
        "missing_buckets": len(todo),
        "seam_buckets": sum(b > dump_end for b in todo),
        "gap_buckets": sum(b < dump_end for b in todo),
        "missing_by_month": by_month,
        "seam": [iso(dump_end + FIVE_MIN), iso(h.live_start)],
        "requests": len(batch),
        "seconds": _seconds(h, len(batch)),
    }
    if dry_run or not batch:
        return report
    for bucket in batch:
        fetch(h, "5m", bucket)
    return {**report, "build": build(h)}


# ---------------------------------------------------------------- upload


def _bytes(h: History, name: str, keep) -> int:
    dt = h.lake.table(name)
    if dt is None:
        return 0
    return sum(
        a["size_bytes"] for a in pa.table(dt.get_add_actions(flatten=True)).to_pylist() if keep(a)
    )


def live_first_bucket(target: Lake) -> datetime | None:
    table = target.read(ingest.BUCKETS, columns=["bucket_start"])
    return pc.min(table.column(0)).as_py() if table is not None and table.num_rows else None


def upload(
    h: History, target: Lake | None, *, dry_run: bool = False, stored_bytes: int | None = None
) -> dict:
    """Copy validated months to `target` (R2): one commit per month for each price table, then
    the manifest and the months table, so a reader never sees a month before its rows.

    `stored_bytes` is the bucket's current storage (from the cost guard); an upload that
    would take it past the M2 limit of 5 GB is refused. Against a lake that already has live
    data, the live manifest must start exactly at `live_start`, so history and live meet with
    neither overlap nor hole."""
    months = built_months(h)
    ok = sorted(m["bucket_month"] for m in months if m["status"] == "validated")
    held = sorted(m["bucket_month"] for m in months if m["status"] != "validated")
    ok_5m = [m for m in ok if month_bounds(m)[1] > h.window_5m_start]
    sizes = {
        PRICES_1H: _bytes(h, PRICES_1H, lambda a: a["partition.bucket_month"] in ok),
        PRICES_5M: _bytes(h, PRICES_5M, lambda a: month_of(a["partition.bucket_date"]) in ok_5m),
        BUCKETS_5M: _bytes(h, BUCKETS_5M, lambda a: True),
        MONTHS: _bytes(h, MONTHS, lambda a: True),
    }
    commits = len(ok) + len(ok_5m) + 2
    report = {
        "months_uploaded": len(ok),
        "months_held_back": held,
        "bytes": {**sizes, "total": sum(sizes.values())},
        "commits": commits,
        "estimated_class_a": commits * OPS_PER_COMMIT[0],
        "estimated_class_b": commits * OPS_PER_COMMIT[1],
    }
    if stored_bytes is not None:
        after = stored_bytes + report["bytes"]["total"]
        report["storage_after_gb"] = round(after / 1e9, 3)
        if after > STORAGE_LIMIT_BYTES:
            raise NotReady(f"upload would take R2 to {after / 1e9:.2f} GB (M2 limit 5 GB)")
    if target is not None:
        live = live_first_bucket(target)
        report["live_first_bucket"] = iso(live) if live else None
        if live is not None and live != h.live_start:
            raise NotReady(f"live data starts {iso(live)}, history ends {iso(h.live_start)}")
    if dry_run:
        return report
    if target is None:
        raise ValueError("no target lake")
    meta = {"task": "history_upload"}
    for month in ok:
        data = h.lake.read(PRICES_1H, filters=[("bucket_month", "=", month)])
        write(
            target,
            PRICES_1H,
            conform(data, PRICES_1H_SCHEMA),
            predicate=f"bucket_month = '{month}'",
            partition_by=["bucket_month"],
            metadata={**meta, "month": month},
        )
    for month in ok_5m:
        m0, m1 = month_bounds(month)
        reader = (
            h.lake.table(PRICES_5M)
            .to_pyarrow_dataset()
            .scanner(
                columns=PRICES_5M_SCHEMA.names,
                filter=(pc.field("bucket_date") >= m0.date())
                & (pc.field("bucket_date") < m1.date()),
            )
            .to_reader()
        )
        write(
            target,
            PRICES_5M,
            reader.cast(PRICES_5M_SCHEMA),
            predicate=f"bucket_date >= '{m0.date()}' AND bucket_date < '{m1.date()}'",
            partition_by=["bucket_date"],
            metadata={**meta, "month": month},
        )
    buckets = conform(h.lake.read(BUCKETS_5M), BUCKETS_SCHEMA)
    write(target, BUCKETS_5M, buckets, partition_by=["bucket_month"], metadata=meta)
    write(target, MONTHS, conform(h.lake.read(MONTHS), MONTHS_SCHEMA), metadata=meta)
    return report


# ---------------------------------------------------------------- the whole run, as a plan


def plan(h: History) -> dict:
    """What a full run would download, request and write, without doing any of it."""
    out: dict = {"download": kaggle.download(h.dump, dry_run=True, pins=h.pins)}
    try:
        out["build"] = build(h, dry_run=True)
    except NotReady as exc:
        month_bytes = sum(p.bytes for p in h.month_pins().values())
        out["build"] = {
            "estimate": str(exc),
            "dump_rows": round(month_bytes * DUMP_ROWS / DUMP_BYTES),
            "hourly_rows": HOURLY_ROWS_ESTIMATE,
            "missing_after_dump_end": SEAM_ESTIMATE,
        }
    try:
        out["check_1h"] = check_1h(h, dry_run=True)
        out["fill"] = fill(h, dry_run=True)
        out["upload"] = upload(h, None, dry_run=True)
    except NotReady as exc:
        seam = out["build"]["missing_after_dump_end"]
        gaps = out["build"].get("missing_buckets", seam) - seam
        out["check_1h"] = {"estimate": str(exc), "requests": CHECK_HOURS * len(h.month_pins())}
        out["fill"] = {"estimate": str(exc), "requests": seam + gaps, "seam_buckets": seam}
        window_bytes = sum(
            p.bytes for m, p in h.month_pins().items() if month_bounds(m)[1] > h.window_5m_start
        )
        rows_5m = round(window_bytes * DUMP_ROWS / DUMP_BYTES)
        out["upload"] = {
            "estimate": str(exc),
            "rows_5m_window": rows_5m,
            "bytes_upper": round((out["build"]["hourly_rows"] + rows_5m) * BYTES_PER_ROW),
        }
    requests = out["check_1h"]["requests"] + out["fill"]["requests"]
    disk = h.root if h.root.exists() else Path.cwd()
    out["totals"] = {
        "kaggle_bytes": out["download"]["bytes_to_download"],
        "wiki_requests": requests,
        "wiki_minutes_at_1_per_second": round(requests * wiki.MIN_INTERVAL_S / 60, 1),
        "local_disk_free_gb": round(shutil.disk_usage(disk).free / 1e9, 1),
    }
    return out
