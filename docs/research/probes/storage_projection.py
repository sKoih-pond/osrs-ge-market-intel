# R2 storage projection for ADR-0019: when the bucket reaches 4, 5 and 6 GB under each option.
#
# Starts from the measured 0.078 GB (cost guard, 21 Oct 2026 18:27 UTC) and adds modelled growth:
# - live data at the hourly-file sizes measured on 19 Oct 2026 (an upper bound: compaction may
#   shrink them; the probe saw 37 to 55% on synthetic data);
# - bronze, 5.9 MB a day, expiring after 30 days;
# - the M2 history upload (1.6 to 1.9 GB), assumed on 15 Nov 2026;
# - superseded files waiting a week for VACUUM, about 0.15 GB;
# - Delta checkpoints, from docs/research/probes/log_growth.py: one per commit, each listing every
#   live file, kept 30 days (delta's default log retention, which cleanup_metadata follows).
# Retention rules take effect from the weekly run of 9 Nov 2026 (if accepted before then).
#
#   python3 docs/research/probes/storage_projection.py
from datetime import date, timedelta

START, S0 = date(2026, 10, 21), 0.078  # GB
GO_LIVE = date(2026, 10, 19)
HISTORY_DAY = date(2026, 11, 15)
RETENTION_FROM = date(2026, 11, 9)
MB = 1e-3  # in GB
GOLD, PRICES, LATEST, OTHER, BRONZE = 7.9, 5.3, 2.9, 0.1, 5.9  # MB a day
IDEA_SHARE = 0.20  # of mart rows: 683 to 1,156 of about 4,220 an hour (16 to 27%) on 20 and 21 Oct
ZSTD_BY_ITEM = 7.74 / 10.3  # closed months of 5-minute prices, measured on a real day
KEPT_FULL = 33.5  # days: 30 plus up to 7 until the next weekly run
OPEN_PRICE_DAYS = 22  # the open month (about 15 days) plus 7 days before it closes
VACUUM_LAG = 0.15
# Checkpoint bytes per live file (KB) and commits an hour, from the probe (full-size schemas).
TABLES = {  # name: (KB per file, commits an hour, partitioned by day)
    "prices_5m": (0.88, 1, True),
    "market_snapshot": (2.7, 1, True),
    "latest_snapshots": (0.88, 1, True),
    "wiki_5m_buckets": (0.64, 1, False),
    "task_runs": (1.07, 2, False),
}


def data_gb(age: float, retention: bool, kept_ideas: float) -> float:
    if not retention:
        return (GOLD + PRICES + LATEST + OTHER) * age * MB
    gold = GOLD * min(age, KEPT_FULL) + GOLD * kept_ideas * max(0.0, age - KEPT_FULL)
    latest = LATEST * min(age, KEPT_FULL)
    prices = PRICES * min(age, OPEN_PRICE_DAYS) + PRICES * ZSTD_BY_ITEM * max(0.0, age - OPEN_PRICE_DAYS)
    return (gold + latest + prices + OTHER * age) * MB


def checkpoints_gb(age: float, retention: bool, every: int) -> float:
    window = min(age, 30.0)  # days of log kept
    total = 0.0
    for name, (kb, per_hour, by_day) in TABLES.items():
        commits = per_hour * 24 * window / every
        if not by_day:
            parts = 1
        elif name == "latest_snapshots" and retention:
            parts = KEPT_FULL
        else:
            parts = max(age - window / 2, 0)  # day partitions, averaged over the log window
        total += kb * commits * (parts + per_hour * 84)  # plus half a week of hourly files
    return total * 1e-6


def model(day: date, retention: bool, every: int, kept_ideas: float, history: float) -> float:
    age = (day - GO_LIVE).days
    rules = retention and day >= RETENTION_FROM
    parts = (
        data_gb(age, rules, kept_ideas)
        + BRONZE * min(age, 30) * MB
        + checkpoints_gb(age, rules, every)
        + (VACUUM_LAG if age >= 7 else 0.0)
        + (history if day >= HISTORY_DAY else 0.0)
    )
    return parts


def projection(retention, every, kept_ideas=IDEA_SHARE, history=1.9, observed=False):
    base = model(START, False, 1, kept_ideas, 0.0)
    out = {}
    day = START
    while day <= date(2029, 12, 31):
        if observed:  # the measured 34 MB a day, unchanged
            gb = S0 + 0.034 * (day - START).days + (history if day >= HISTORY_DAY else 0.0)
        else:
            gb = S0 + model(day, retention, every, kept_ideas, history) - base
        out[day] = gb
        day += timedelta(days=1)
    return out


def when(series, limit):
    return next((d for d, gb in series.items() if gb >= limit), None)


SCENARIOS = [
    ("measured 34 MB a day continues", dict(retention=False, every=1, observed=True)),
    ("no change (model)", dict(retention=False, every=1)),
    ("retention rules A to C, checkpoint every commit", dict(retention=True, every=1)),
    ("retention rules A to C, published ideas only (2.4%)", dict(retention=True, every=1, kept_ideas=0.024)),
    ("checkpoint every 10th commit only (rule D)", dict(retention=False, every=10)),
    ("retention rules A to D", dict(retention=True, every=10)),
]
for history in (1.9, 1.6):
    print(f"\nHistory upload {history} GB on {HISTORY_DAY}")
    print(f"{'scenario':58} {'4 GB':>10} {'5 GB':>10} {'6 GB':>10} {'31 Oct 27':>9} {'31 Oct 28':>9}")
    for label, kw in SCENARIOS:
        s = projection(history=history, **kw)
        cells = [str(when(s, g) or "after 2029") for g in (4, 5, 6)]
        print(f"{label:58} {cells[0]:>10} {cells[1]:>10} {cells[2]:>10} "
              f"{s[date(2027, 10, 31)]:9.2f} {s[date(2028, 10, 31)]:9.2f}")
print("\nModel parts on 31 Oct 2027 (GB), no change vs rules A to D:")
for label, r, e in (("no change", False, 1), ("A to D", True, 10)):
    age = (date(2027, 10, 31) - GO_LIVE).days
    print(f"  {label:10} data {data_gb(age, r, IDEA_SHARE):.2f}, checkpoints {checkpoints_gb(age, r, e):.2f}, "
          f"bronze {BRONZE * 30 * MB:.2f}, VACUUM lag {VACUUM_LAG:.2f}")
