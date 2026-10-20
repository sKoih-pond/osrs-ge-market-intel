-- Rows returned here break a money rule; the test passes when none are returned.
select item_id, buy_price, sell_price, tax_per_item, net_margin, feasible_quantity, buy_limit
from {{ ref('market_snapshot') }}
where net_margin > sell_price - buy_price                        -- net never exceeds gross
   or tax_per_item < 0 or tax_per_item > 5000000                 -- cap per item (ADR-0007)
   or (tax_exempt and tax_per_item <> 0)                         -- exemption honoured
   or (sell_price < 50 and tax_per_item <> 0)                    -- 2% rounds to zero below 50 GP
   or (buy_limit is not null and feasible_quantity > buy_limit)  -- buy limit respected
   or feasible_quantity < 0
