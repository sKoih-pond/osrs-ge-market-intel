"""Fetch raw Wiki responses to local disk for fixtures and threshold derivation.

Usage: python scripts/fetch_raw.py --end 2026-10-19T22:00:00Z --hours-5m 24 --hours-1h 336

Writes gzipped bodies under data/raw/ (git-ignored), skipping files already present, so it
can be re-run safely. One request per second, explicit timestamps, descriptive User-Agent.
"""

from __future__ import annotations

import argparse
import gzip
from datetime import datetime, timedelta
from pathlib import Path

from ge_intel.wiki import WikiClient

OUT = Path("data/raw")


def save(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(body, mtime=0))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", required=True, type=datetime.fromisoformat)
    ap.add_argument("--hours-5m", type=int, default=24)
    ap.add_argument("--hours-1h", type=int, default=0)
    ap.add_argument("--latest-and-mapping", action="store_true")
    args = ap.parse_args()
    client = WikiClient()
    end = args.end
    if args.latest_and_mapping:
        save(OUT / "latest" / f"{int(end.timestamp())}.json.gz", client.latest().body)
        save(OUT / "mapping" / f"{int(end.timestamp())}.json.gz", client.mapping().body)
    for step, hours, delta in (
        ("5m", args.hours_5m, timedelta(minutes=5)),
        ("1h", args.hours_1h, timedelta(hours=1)),
    ):
        t = end - timedelta(hours=hours)
        while t < end:
            path = OUT / step / f"{int(t.timestamp())}.json.gz"
            if not path.exists():
                save(path, client.bucket(step, t).body)
            t += delta
    print(f"done, {client.requests_sent} requests")


if __name__ == "__main__":
    main()
