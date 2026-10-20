"""Era-aware tax exemptions and regimes from the seeds (ADR-0006, ADR-0007)."""

import csv
from datetime import UTC, datetime

import pytest

from ge_intel import reference
from ge_intel.reference import SEEDS, is_exempt, item_tax_per_item, regime_at

BOND, LOBSTER, WATERING_CAN_0, WATERING_CAN_8, SHARK, TWISTED_BOW = (
    13190,
    379,
    5331,
    5340,
    385,
    20997,
)
BEFORE_TAX = datetime(2021, 12, 8, 12, tzinfo=UTC)
ERA_2021 = datetime(2021, 12, 9, tzinfo=UTC)
LAST_1PCT_DAY = datetime(2025, 5, 28, 23, 59, tzinfo=UTC)
ERA_2025 = datetime(2025, 5, 29, tzinfo=UTC)
NOW = datetime(2026, 10, 19, 22, tzinfo=UTC)


def test_bonds_and_tools_are_exempt_from_the_first_tax_day():
    assert not is_exempt(BOND, BEFORE_TAX)  # no tax yet, so no exemption to record
    assert is_exempt(BOND, ERA_2021) and is_exempt(BOND, NOW)
    assert is_exempt(WATERING_CAN_0, ERA_2021)
    assert not is_exempt(WATERING_CAN_8, NOW)  # only the empty can is on the 2021 list


def test_2025_additions_start_on_29_may_2025():
    assert not is_exempt(LOBSTER, LAST_1PCT_DAY)
    assert is_exempt(LOBSTER, ERA_2025)
    assert item_tax_per_item(LOBSTER, 200, LAST_1PCT_DAY) == 2  # 1%, not yet exempt
    assert item_tax_per_item(LOBSTER, 200, ERA_2025) == 0


def test_taxed_items_follow_the_rate_and_cap():
    assert item_tax_per_item(SHARK, 1_000, ERA_2021) == 10
    assert item_tax_per_item(SHARK, 1_000, NOW) == 20
    assert item_tax_per_item(TWISTED_BOW, 1_400_000_000, NOW) == 5_000_000


def test_exemption_needs_an_aware_time():
    with pytest.raises(ValueError):
        is_exempt(BOND, datetime(2026, 1, 1))


def test_seed_rows_are_unique_and_sourced():
    with (SEEDS / "tax_exemptions.csv").open() as fh:
        rows = list(csv.DictReader(fh))
    assert len({r["item_id"] for r in rows}) == len(rows) == 48
    assert all(r["source_url"].startswith("https://oldschool.runescape.wiki/") for r in rows)
    assert {r["exempt_from"] for r in rows} == {"2021-12-09", "2025-05-29"}


def test_measurement_regime_switches_on_30_september_2026():
    assert (
        regime_at("measurement", datetime(2026, 9, 29, 23, 55, tzinfo=UTC))
        == "measurement-runelite"
    )
    assert regime_at("measurement", datetime(2026, 9, 30, tzinfo=UTC)) == "measurement-jagex"
    with pytest.raises(LookupError):
        regime_at("measurement", datetime(2020, 1, 1, tzinfo=UTC))


def test_regime_seed_has_no_overlaps_within_a_kind():
    by_kind = {}
    for r in reference.regimes():
        by_kind.setdefault(r.kind, []).append(r)
    for rows in by_kind.values():
        rows.sort(key=lambda r: r.starts_at)
        for a, b in zip(rows, rows[1:], strict=False):
            assert a.ends_at is not None and a.ends_at <= b.starts_at
