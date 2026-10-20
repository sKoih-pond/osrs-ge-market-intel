"""dbt-duckdb plugin: the tax rule (ADR-0007) as a SQL macro, `ge_tax_per_item`.

The macro is generated from `ge_intel.tax` (its eras and cap), so the rule has one source;
`tests/test_dbt_plugin.py` checks the macro against the Python function on the edge cases.
Times cross the boundary as epoch seconds to avoid time-zone conversion.
"""

from __future__ import annotations

from dbt.adapters.duckdb.plugins import BasePlugin

from ge_intel import tax


def tax_macro_sql() -> str:
    rate = "0"
    for era in tax.TAX_ERAS:  # oldest first; each later era wraps the earlier ones
        since = int(era.starts_at.timestamp())
        rate = f"CASE WHEN placed_epoch >= {since} THEN {era.rate_bp} ELSE {rate} END"
    return (
        "CREATE OR REPLACE MACRO ge_tax_per_item(price, placed_epoch, exempt) AS "
        "CASE WHEN price IS NULL OR placed_epoch IS NULL THEN NULL "
        "WHEN price < 0 THEN error('price must not be negative') "
        "WHEN exempt THEN 0 "
        f"ELSE least((CAST(price AS BIGINT) * ({rate})) // 10000, {tax.TAX_CAP_PER_ITEM}) END"
    )


class Plugin(BasePlugin):
    def configure_connection(self, conn):
        conn.execute(tax_macro_sql())
