from datetime import UTC, datetime

import duckdb
import pytest

from ge_intel import tax
from ge_intel.dbt_plugin import tax_macro_sql

PRICES = [
    0,
    1,
    49,
    50,
    51,
    99,
    100,
    101,
    4_999,
    5_000,
    123_456,
    249_999_999,
    250_000_000,
    499_999_999,
    500_000_000,
    1_500_000_000,
    2_147_483_647,
    900_000_000_000,
]
TIMES = [
    datetime(2021, 12, 8, 23, 59, tzinfo=UTC),
    datetime(2021, 12, 9, tzinfo=UTC),
    datetime(2025, 5, 28, 23, 59, tzinfo=UTC),
    datetime(2025, 5, 29, tzinfo=UTC),
    datetime(2026, 10, 19, 22, tzinfo=UTC),
]


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(tax_macro_sql())
    return c


@pytest.mark.parametrize("when", TIMES)
@pytest.mark.parametrize("exempt", [False, True])
def test_sql_macro_matches_python_tax(con, when, exempt):
    epoch = int(when.timestamp())
    for price in PRICES:
        got = con.execute(
            "select ge_tax_per_item(?::bigint, ?::bigint, ?)", [price, epoch, exempt]
        ).fetchone()[0]
        assert got == tax.tax_per_item(price, when, exempt=exempt), (price, when, exempt)


def test_sql_macro_passes_nulls_through(con):
    assert (
        con.execute("select ge_tax_per_item(NULL::bigint, 1::bigint, false)").fetchone()[0] is None
    )


def test_sql_macro_refuses_negative_prices(con):
    with pytest.raises(duckdb.Error):
        con.execute("select ge_tax_per_item(-1::bigint, 1::bigint, false)").fetchone()
