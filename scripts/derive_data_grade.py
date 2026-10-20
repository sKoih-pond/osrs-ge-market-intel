"""Reproduce the tables behind data-grade-v1 and the outage rule (docs/methodology/).

Usage: python scripts/derive_data_grade.py   (reads data/raw/1h and data/raw/5m from
scripts/fetch_raw.py; prints Markdown tables)

Question asked of the data: when the screener quotes a price from the last hour, how far do
the next hour's prices move, and does the item keep trading on both sides? Both are measured
against the inputs the grade can see at decision time.
"""

from __future__ import annotations

import gzip
import json
import statistics
from collections import defaultdict
from pathlib import Path

RAW = Path("data/raw")


def load(step: str) -> list[dict]:
    return [
        json.loads(gzip.decompress(f.read_bytes()))["data"]
        for f in sorted((RAW / step).glob("*.json.gz"))
    ]


def row(*cells) -> None:
    print("| " + " | ".join(str(c) for c in cells) + " |")


def two_sided(rec) -> bool:
    return rec.get("avgHighPrice") is not None and rec.get("avgLowPrice") is not None


def drift(price: tuple[float, float], nxt: tuple[float, float]) -> float:
    return max(abs(nxt[0] / price[0] - 1), abs(nxt[1] / price[1] - 1))


def coverage_table(hours: list[dict]) -> None:
    by_cov: dict[int, list] = defaultdict(list)
    by_vol: dict[tuple[int, int], list] = defaultdict(list)
    bins = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 10**12]
    for k in range(24, len(hours) - 1):
        for item, rec in hours[k - 1].items():
            if not two_sided(rec):
                continue
            cov = sum(1 for j in range(k - 24, k) if item in hours[j] and two_sided(hours[j][item]))
            nxt = hours[k].get(item)
            err = None
            if nxt and two_sided(nxt):
                err = drift(
                    (rec["avgHighPrice"], rec["avgLowPrice"]),
                    (nxt["avgHighPrice"], nxt["avgLowPrice"]),
                )
            by_cov[cov].append(err)
            if cov == 24:
                vol = min(rec["highPriceVolume"], rec["lowPriceVolume"])
                lo = max(b for b in bins if b <= vol)
                by_vol[(lo, bins[bins.index(lo) + 1])].append(err)
    row(
        "Two-sided hours in last 24", "Item-hours", "Median next-hour drift", "Both sides next hour"
    )
    print("|---:|---:|---:|---:|")
    for cov in sorted(by_cov):
        errs = [e for e in by_cov[cov] if e is not None]
        n = len(by_cov[cov])
        row(cov, f"{n:,}", f"{statistics.median(errs) * 100:.2f}%", f"{len(errs) / n:.1%}")
    print("\nAt 24 of 24 hours, by the thinner side's volume in the price hour:\n")
    print("| Volume | Item-hours | Median drift | Drift within 2% |")
    print("|---|---:|---:|---:|")
    for (lo, hi), vals in sorted(by_vol.items()):
        errs = [e for e in vals if e is not None]
        label = f"{lo:,} or more" if hi == 10**12 else f"{lo:,} to {hi - 1:,}"
        within = sum(e <= 0.02 for e in errs) / len(vals)
        row(label, f"{len(vals):,}", f"{statistics.median(errs) * 100:.2f}%", f"{within:.1%}")


def envelope_table(hours: list[dict]) -> None:
    """Distance of the price hour from the item's own 24-hour median (data-grade-v2)."""
    bands = [1, 1.02, 1.05, 1.1, 1.15, 1.25, 1.5, 2, 3, float("inf")]
    by_band: dict[float, list] = defaultdict(list)
    for k in range(24, len(hours) - 1):
        for item, rec in hours[k - 1].items():
            if not two_sided(rec):
                continue
            window = [hours[j][item] for j in range(k - 24, k) if item in hours[j]]
            if sum(two_sided(w) for w in window) < 16:
                continue
            ratios = []
            for side in ("avgHighPrice", "avgLowPrice"):
                median = statistics.median(w[side] for w in window if w.get(side) is not None)
                ratios.append(max(rec[side] / median, median / rec[side]))
            nxt = hours[k].get(item)
            err = None
            if nxt and two_sided(nxt):
                err = drift(
                    (rec["avgHighPrice"], rec["avgLowPrice"]),
                    (nxt["avgHighPrice"], nxt["avgLowPrice"]),
                )
            by_band[max(b for b in bands if b <= max(ratios))].append(err)
    print()
    row("Price hour vs own 24-hour median", "Item-hours", "Median next-hour drift")
    print("|---|---:|---:|")
    for lo, hi in zip(bands, bands[1:], strict=False):
        vals = by_band.get(lo, [])
        errs = [e for e in vals if e is not None]
        if errs:
            label = f"x{lo} to x{hi}" if hi != float("inf") else f"x{lo} or more"
            row(label, f"{len(vals):,}", f"{statistics.median(errs) * 100:.2f}%")


def staleness_table(buckets: list[dict]) -> None:
    def vwap(lo: int, hi: int) -> dict[str, tuple[float, float]]:
        acc: dict[str, list[float]] = {}
        for d in buckets[lo:hi]:
            for item, r in d.items():
                a = acc.setdefault(item, [0, 0, 0, 0])
                if r.get("avgHighPrice") is not None:
                    a[0] += r["avgHighPrice"] * r["highPriceVolume"]
                    a[1] += r["highPriceVolume"]
                if r.get("avgLowPrice") is not None:
                    a[2] += r["avgLowPrice"] * r["lowPriceVolume"]
                    a[3] += r["lowPriceVolume"]
        return {i: (a[0] / a[1], a[2] / a[3]) for i, a in acc.items() if a[1] and a[3]}

    rows = []
    for k in range(12, len(buckets) - 12, 12):
        price, nxt = vwap(k - 12, k), vwap(k, k + 12)
        for item, p in price.items():
            last_high = next(
                j
                for j in range(k - 1, k - 13, -1)
                if buckets[j].get(item, {}).get("avgHighPrice") is not None
            )
            last_low = next(
                j
                for j in range(k - 1, k - 13, -1)
                if buckets[j].get(item, {}).get("avgLowPrice") is not None
            )
            minutes = (k - min(last_high, last_low)) * 5 - 2.5  # bucket midpoint
            rows.append((minutes, drift(p, nxt[item]) if item in nxt else None))
    print()
    row(
        "Minutes since the staler side traded", "Item-hours", "Median drift", "Both sides next hour"
    )
    print("|---|---:|---:|---:|")
    for lo, hi in [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 45), (45, 61)]:
        v = [e for m, e in rows if lo <= m < hi]
        errs = [e for e in v if e is not None]
        row(
            f"{lo} to {hi}",
            f"{len(v):,}",
            f"{statistics.median(errs) * 100:.2f}%",
            f"{len(errs) / len(v):.1%}",
        )


def outage_table(buckets: list[dict]) -> None:
    counts = [len(d) for d in buckets]
    median = statistics.median(counts)
    print(
        f"\n5-minute buckets: {len(counts)}, median items {median:,.0f}, lowest {min(counts):,} "
        f"({min(counts) / median:.2f} of the median)"
    )


if __name__ == "__main__":
    hourly = load("1h")
    coverage_table(hourly)
    envelope_table(hourly)
    five = load("5m")[-288:]  # the 24-hour window before the fixture hour
    staleness_table(five)
    outage_table(five)
