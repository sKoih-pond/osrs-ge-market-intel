"""Build the M2 history fixtures from the recorded Wiki responses.

Usage: python scripts/make_history_fixtures.py

- `tests/fixtures/wiki/1h.json.gz`: the real `/1h` responses (data/raw/1h, from
  scripts/fetch_raw.py) for the 24 hours of the 5-minute fixture, trimmed to its items.
- `tests/fixtures/kaggle/prices_5m/year=2026/month=10/data.parquet`: the 5-minute fixture
  written the way the Kaggle dump `antonygarand/osrs-ge-5m` v1 writes a month: Wiki field names,
  `timestamp` as a bucket number (epoch seconds / 300), double prices, sorted by item then
  time, zstd. Like the real dump it ends mid-hour (19 Oct 19:40 UTC) and misses buckets, so the
  tests have a seam and gaps to fill. Real data, reshaped; not a copy of the dump.

`tests/fixtures/kaggle/README.md` is the dump's own README, downloaded on 21 October 2026
(SHA-256 pinned in src/ge_intel/sources/kaggle_osrs_ge_5m_v1.json).
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RAW = Path("data/raw")
WIKI = Path("tests/fixtures/wiki")
KAGGLE = Path("tests/fixtures/kaggle")
DUMP_LAST_BUCKET = datetime(2026, 10, 19, 19, 40, tzinfo=UTC)
# Missing from the "dump": two in one hour, and one the API is also made to return empty.
DUMP_GAPS = (
    datetime(2026, 10, 19, 3, 10, tzinfo=UTC),
    datetime(2026, 10, 19, 3, 15, tzinfo=UTC),
    datetime(2026, 10, 19, 11, 30, tzinfo=UTC),
)

DUMP_SCHEMA = pa.schema(
    [
        ("itemId", pa.int32()),
        ("timestamp", pa.int32()),
        ("avgLowPrice", pa.float64()),
        ("avgHighPrice", pa.float64()),
        ("lowPriceVolume", pa.int64()),
        ("highPriceVolume", pa.int64()),
    ]
)


def load(path: Path):
    return json.loads(gzip.decompress(path.read_bytes()))


def main() -> None:
    meta = json.loads((WIKI / "fixtures.json").read_text())
    keep = {str(i) for i in meta["items"]}
    end = datetime.fromisoformat(meta["end"])
    buckets = load(WIKI / "5m.json.gz")

    hours = [end - timedelta(hours=24) + timedelta(hours=i) for i in range(24)]
    one_h = {}
    for h in hours:
        payload = load(RAW / "1h" / f"{int(h.timestamp())}.json.gz")
        one_h[str(int(h.timestamp()))] = {
            "data": {k: v for k, v in payload["data"].items() if k in keep},
            "timestamp": payload["timestamp"],
        }
    (WIKI / "1h.json.gz").write_bytes(
        gzip.compress(json.dumps(one_h, sort_keys=True).encode(), mtime=0)
    )

    gaps = {int(t.timestamp()) for t in DUMP_GAPS}
    rows = []
    for ts, payload in buckets.items():
        epoch = int(ts)
        if epoch > DUMP_LAST_BUCKET.timestamp() or epoch in gaps:
            continue
        for key, rec in payload["data"].items():
            rows.append(
                {
                    "itemId": int(key),
                    "timestamp": epoch // 300,
                    "avgLowPrice": rec.get("avgLowPrice"),
                    "avgHighPrice": rec.get("avgHighPrice"),
                    "lowPriceVolume": rec.get("lowPriceVolume", 0),
                    "highPriceVolume": rec.get("highPriceVolume", 0),
                }
            )
    rows.sort(key=lambda r: (r["itemId"], r["timestamp"]))
    out = KAGGLE / "prices_5m" / "year=2026" / "month=10" / "data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=DUMP_SCHEMA), out, compression="zstd")
    print(f"{len(one_h)} hours of /1h, {len(rows)} dump rows")


if __name__ == "__main__":
    main()
