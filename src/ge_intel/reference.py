"""Seed-backed reference data: regimes (ADR-0006) and tax exemptions by era (ADR-0007)."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import cache
from pathlib import Path

from ge_intel import tax
from ge_intel.timegrid import parse_utc

SEEDS = Path(__file__).parent / "seeds"


@dataclass(frozen=True)
class Regime:
    regime_id: str
    kind: str
    starts_at: datetime
    ends_at: datetime | None


@cache
def regimes() -> tuple[Regime, ...]:
    with (SEEDS / "regimes.csv").open() as fh:
        return tuple(
            Regime(
                r["regime_id"],
                r["kind"],
                parse_utc(r["starts_at"]),
                parse_utc(r["ends_at"]) if r["ends_at"] else None,
            )
            for r in csv.DictReader(fh)
        )


def regime_at(kind: str, moment: datetime) -> str:
    """The id of the `kind` regime in force at `moment`; raises if none covers it."""
    for r in regimes():
        if r.kind == kind and r.starts_at <= moment and (r.ends_at is None or moment < r.ends_at):
            return r.regime_id
    raise LookupError(f"no {kind} regime covers {moment.isoformat()}")


@cache
def _exemptions() -> tuple[tuple[int, date, date | None], ...]:
    with (SEEDS / "tax_exemptions.csv").open() as fh:
        return tuple(
            (
                int(r["item_id"]),
                date.fromisoformat(r["exempt_from"]),
                date.fromisoformat(r["exempt_to"]) if r["exempt_to"] else None,
            )
            for r in csv.DictReader(fh)
        )


def is_exempt(item_id: int, offer_placed_at: datetime) -> bool:
    """Whether a sell offer for `item_id` placed at that time paid no tax.

    Eras start at 00:00 UTC on the update day, the same rule as the tax rate (ADR-0007).
    """
    if offer_placed_at.tzinfo is None:
        raise ValueError("offer_placed_at must be timezone-aware (UTC)")
    day = offer_placed_at.astimezone(UTC).date()
    return any(
        i == item_id and start <= day and (end is None or day < end)
        for i, start, end in _exemptions()
    )


def item_tax_per_item(item_id: int, price: int, offer_placed_at: datetime) -> int:
    """Tax per item for this item, applying the exemption list of the offer's era."""
    exempt = is_exempt(item_id, offer_placed_at)
    return tax.tax_per_item(price, offer_placed_at, exempt=exempt)
