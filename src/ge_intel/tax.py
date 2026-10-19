"""Grand Exchange tax rules by era (ADR-0007).

Rules from Jagex's update notes as copied on the OSRS Wiki, read 2026-10-20:
- 9 Dec 2021: 1% per item, rounded down, capped at 5,000,000 GP per item.
- 29 May 2025: 2% for new offers, same rounding and cap.
The seller pays: tax is deducted from the sale proceeds. Exemptions are era-specific
and decided by the caller (seed built in M1).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

TAX_CAP_PER_ITEM = 5_000_000
_BASIS_POINTS = 10_000


@dataclass(frozen=True)
class TaxEra:
    starts_at: datetime  # inclusive, UTC; applied at 00:00 on the update day
    rate_bp: int  # basis points: 100 = 1%


# Oldest first. Offers placed before the first era paid no tax.
TAX_ERAS: tuple[TaxEra, ...] = (
    TaxEra(datetime(2021, 12, 9, tzinfo=UTC), 100),
    TaxEra(datetime(2025, 5, 29, tzinfo=UTC), 200),
)


def _require_aware(moment: datetime) -> None:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("offer_placed_at must be timezone-aware (UTC)")


def _require_whole(name: str, value: int) -> None:
    # bool is an int subclass; reject it so True/False never pass as coins or counts.
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be a whole number")
    if value < 0:
        raise ValueError(f"{name} must not be negative")


def tax_rate_bp(offer_placed_at: datetime) -> int:
    """Tax rate in basis points for a sell offer placed at the given time."""
    _require_aware(offer_placed_at)
    rate = 0
    for era in TAX_ERAS:
        if offer_placed_at >= era.starts_at:
            rate = era.rate_bp
    return rate


def tax_per_item(price: int, offer_placed_at: datetime, *, exempt: bool = False) -> int:
    """Coins of tax on one item sold at `price` (the transaction price, in whole GP)."""
    _require_whole("price", price)
    rate = tax_rate_bp(offer_placed_at)
    if exempt:
        return 0
    return min(price * rate // _BASIS_POINTS, TAX_CAP_PER_ITEM)


def net_proceeds(
    price: int, quantity: int, offer_placed_at: datetime, *, exempt: bool = False
) -> int:
    """Coins the seller receives for `quantity` items sold at `price` each."""
    _require_whole("quantity", quantity)
    return (price - tax_per_item(price, offer_placed_at, exempt=exempt)) * quantity
