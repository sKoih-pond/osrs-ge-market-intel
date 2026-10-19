from datetime import UTC, datetime, timedelta, timezone

import pytest

from ge_intel.tax import TAX_CAP_PER_ITEM, net_proceeds, tax_per_item, tax_rate_bp

PRE_TAX = datetime(2021, 12, 8, 23, 59, tzinfo=UTC)
ONE_PCT = datetime(2023, 6, 1, 12, 0, tzinfo=UTC)
TWO_PCT = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("moment", "rate"),
    [
        (PRE_TAX, 0),
        (datetime(2021, 12, 9, tzinfo=UTC), 100),
        (datetime(2025, 5, 28, 23, 59, 59, tzinfo=UTC), 100),
        (datetime(2025, 5, 29, tzinfo=UTC), 200),
        # Same instant expressed in UTC+10 must give the same rate.
        (datetime(2025, 5, 29, 10, 0, tzinfo=timezone(timedelta(hours=10))), 200),
    ],
)
def test_rate_follows_offer_time(moment, rate):
    assert tax_rate_bp(moment) == rate


@pytest.mark.parametrize(
    ("price", "moment", "expected"),
    [
        # 2%: below 50 GP rounds to zero (wiki: 49 or 50 both net 49 to the seller).
        (49, TWO_PCT, 0),
        (50, TWO_PCT, 1),
        (99, TWO_PCT, 1),
        (100, TWO_PCT, 2),
        # 1%: below 100 GP rounds to zero; Jagex's own examples (975 -> 9, 101 -> 1).
        (99, ONE_PCT, 0),
        (101, ONE_PCT, 1),
        (975, ONE_PCT, 9),
        # Cap of 5M per item, in both eras.
        (250_000_000, TWO_PCT, 5_000_000),
        (3_000_000_000, TWO_PCT, TAX_CAP_PER_ITEM),
        (870_000_000, ONE_PCT, TAX_CAP_PER_ITEM),
        (5_000, PRE_TAX, 0),
    ],
)
def test_tax_per_item(price, moment, expected):
    assert tax_per_item(price, moment) == expected


def test_exempt_items_pay_nothing():
    assert tax_per_item(1_000_000, TWO_PCT, exempt=True) == 0


def test_tax_is_per_item_not_per_offer():
    # 100 sharks at 975 GP in the 1% era: 9 GP each, not floor(97,500 * 1%) = 975.
    assert net_proceeds(975, 100, ONE_PCT) == (975 - 9) * 100


def test_tax_never_exceeds_price_and_is_monotonic():
    previous = 0
    for price in range(0, 20_001):
        tax = tax_per_item(price, TWO_PCT)
        assert 0 <= tax <= price
        assert tax >= previous
        previous = tax


@pytest.mark.parametrize("bad_price", [50.0, 49.99, True, "50"])
def test_rejects_non_whole_prices(bad_price):
    with pytest.raises(TypeError):
        tax_per_item(bad_price, TWO_PCT)


def test_rejects_negative_values():
    with pytest.raises(ValueError):
        tax_per_item(-1, TWO_PCT)
    with pytest.raises(ValueError):
        net_proceeds(100, -1, TWO_PCT)


def test_rejects_naive_datetimes():
    with pytest.raises(ValueError):
        tax_rate_bp(datetime(2026, 1, 1))
