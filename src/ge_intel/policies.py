"""Versioned definitions the mart is built with: execution policies (ADR-0008) and the Data
grade (ADR-0009). Changing any value means a new version id, never an edit in place.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Policy:
    id: str
    price_window_hours: int  # reference prices: trailing VWAP over this many hours
    entry_window_hours: int  # buy offer may fill within this window
    exit_horizon_hours: int  # position closed by
    queue_share: float  # our quantity at most this share of the side's volume
    capital_gp: int  # default capital; adjustable in the browser later (10M to 500M)

    def as_vars(self) -> dict:
        return asdict(self)


CASUAL_V1 = Policy(
    id="casual-v1",
    price_window_hours=1,
    entry_window_hours=4,
    exit_horizon_hours=24,
    queue_share=0.10,
    capital_gp=50_000_000,
)


@dataclass(frozen=True)
class DataGrade:
    """Thresholds derived from data; see docs/methodology/data_grade.md for the derivation."""

    version: str
    a_min_two_sided_hours: int
    a_max_minutes_since_trade: int
    a_max_price_deviation_pct: int
    c_below_two_sided_hours: int
    c_above_minutes_since_trade: int
    c_above_price_deviation_pct: int
    min_window_buckets: int  # recorded buckets (traded or outage) of the 288 in 24 hours

    def as_vars(self) -> dict:
        return asdict(self)


# Derived on 14 days of post-switch /1h data and 24 hours of /5m data (680,069 and 48,257
# item-hours): docs/methodology/data_grade.md. Volume was tested and did not separate stable
# from unstable prices, so there is no volume threshold. v2 (20 Oct 2026) adds the price
# envelope after v1 published a single stray trade (Yellow boots at 85 times its usual price)
# as the top idea in its first live hour.
DATA_GRADE_V2 = DataGrade(
    version="data-grade-v2",
    a_min_two_sided_hours=24,
    a_max_minutes_since_trade=10,
    a_max_price_deviation_pct=5,
    c_below_two_sided_hours=16,
    c_above_minutes_since_trade=30,
    c_above_price_deviation_pct=10,
    min_window_buckets=259,  # 90% of 288; an operating rule, not a derived threshold
)
