"""Trim raw Wiki responses (data/raw/, from scripts/fetch_raw.py) into test fixtures.

Usage: python scripts/make_fixtures.py --end 2026-10-19T22:00:00Z

Keeps a fixed set of edge-case items plus a seeded sample across liquidity tiers, for the 24
hours before --end. The output is real OSRS Wiki data (CC BY-NC-SA 3.0), so it lives in
tests/fixtures/wiki/ beside its own licence notice.
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

RAW = Path("data/raw")
OUT = Path("tests/fixtures/wiki")
EDGE_CASES = {
    13190: "Old school bond: exempt since 2021",
    379: "Lobster: exempt since 2025",
    558: "Mind rune: exempt since 2025, under 50 GP",
    2552: "Ring of dueling(8): exempt since 2025",
    3008: "Energy potion(4): exempt since 2025 (all doses assumed)",
    20997: "Twisted bow: tax cap applies",
    27277: "Tumeken's shadow (uncharged): tax cap applies",
    314: "Feather: under 50 GP, no tax at 2%",
    561: "Nature rune: very liquid",
    453: "Coal: very liquid",
    385: "Shark",
    4151: "Abyssal whip",
    3024: "Super restore(4)",
    12934: "Zulrah's scales",
    536: "Dragon bones",
}
SAMPLE_PER_TIER = 12


def load(path: Path):
    return json.loads(gzip.decompress(path.read_bytes()))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", required=True, type=datetime.fromisoformat)
    end = ap.parse_args().end
    starts = [end - timedelta(hours=24) + i * timedelta(minutes=5) for i in range(288)]
    buckets = {
        int(t.timestamp()): load(RAW / "5m" / f"{int(t.timestamp())}.json.gz") for t in starts
    }
    latest_file = sorted((RAW / "latest").glob("*.json.gz"))[-1]
    mapping_file = sorted((RAW / "mapping").glob("*.json.gz"))[-1]
    latest, mapping = load(latest_file), load(mapping_file)
    mapped = {r["id"] for r in mapping}

    volume: dict[int, int] = {}
    for payload in buckets.values():
        for key, rec in payload["data"].items():
            volume[int(key)] = (
                volume.get(int(key), 0) + rec["highPriceVolume"] + rec["lowPriceVolume"]
            )
    ranked = sorted((i for i in volume if i in mapped and i not in EDGE_CASES), key=volume.get)
    rng = random.Random(20261020)
    chosen = set(EDGE_CASES)
    quartile = len(ranked) // 4
    for q in range(4):
        chosen.update(rng.sample(ranked[q * quartile : (q + 1) * quartile], SAMPLE_PER_TIER))
    no_limit = [i for i in ranked if not any(r["id"] == i and "limit" in r for r in mapping)]
    chosen.update(no_limit[-3:])  # traded items with no published buy limit
    keep = {str(i) for i in chosen}

    OUT.mkdir(parents=True, exist_ok=True)
    trimmed = {
        str(ts): {
            "data": {k: v for k, v in p["data"].items() if k in keep},
            "timestamp": p["timestamp"],
        }
        for ts, p in buckets.items()
    }
    (OUT / "5m.json.gz").write_bytes(
        gzip.compress(json.dumps(trimmed, sort_keys=True).encode(), mtime=0)
    )
    (OUT / "latest.json.gz").write_bytes(
        gzip.compress(
            json.dumps({"data": {k: v for k, v in latest["data"].items() if k in keep}}).encode(),
            mtime=0,
        )
    )
    (OUT / "mapping.json.gz").write_bytes(
        gzip.compress(json.dumps([r for r in mapping if str(r["id"]) in keep]).encode(), mtime=0)
    )
    meta = {
        "end": end.isoformat(),
        "latest_fetched": latest_file.stem.split(".")[0],
        "mapping_fetched": mapping_file.stem.split(".")[0],
        "items": sorted(chosen),
        "edge_cases": {str(k): v for k, v in EDGE_CASES.items()},
    }
    (OUT / "fixtures.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"{len(chosen)} items, {len(trimmed)} buckets")


if __name__ == "__main__":
    main()
