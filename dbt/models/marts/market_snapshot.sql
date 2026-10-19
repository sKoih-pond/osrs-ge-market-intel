-- Market snapshot for one hour under one execution policy (ADR-0008), with the Data grade
-- (ADR-0009). One row per item that traded on either side in the trailing 24 hours.
{% set p = var('policy') %}
{% set g = var('grade') %}
{% set snap = "timestamptz '" ~ var('snapshot_at') ~ "'" %}

with prices as (
    select * from {{ ref('stg_prices_5m') }}
),

price_window as (  -- policy reference window, 1 hour for casual-v1
    select
        item_id,
        sum(avg_high_price * high_volume) filter (where avg_high_price is not null)
            / nullif(sum(high_volume) filter (where avg_high_price is not null), 0) as instant_buy_vwap,
        sum(avg_low_price * low_volume) filter (where avg_low_price is not null)
            / nullif(sum(low_volume) filter (where avg_low_price is not null), 0) as instant_sell_vwap,
        coalesce(sum(high_volume) filter (where avg_high_price is not null), 0) as instant_buy_volume,
        coalesce(sum(low_volume) filter (where avg_low_price is not null), 0) as instant_sell_volume
    from prices
    where bucket_start >= {{ snap }} - interval {{ p['price_window_hours'] }} hour
    group by item_id
),

entry_window as (  -- volume the buy offer competes for, 4 hours for casual-v1
    select item_id, coalesce(sum(low_volume), 0) as instant_sell_volume_entry
    from prices
    where bucket_start >= {{ snap }} - interval {{ p['entry_window_hours'] }} hour
    group by item_id
),

hours as (
    select
        item_id,
        date_trunc('hour', bucket_start) as hour_start,
        bool_or(avg_high_price is not null) as buy_side,
        bool_or(avg_low_price is not null) as sell_side
    from prices
    group by 1, 2
),

coverage as (
    select item_id, count(*) filter (where buy_side and sell_side) as two_sided_hours_24h
    from hours
    group by item_id
),

window_health as (  -- one row: how complete the 24-hour window is
    select
        count(*) filter (where status = 'ok') as buckets_ok_24h,
        count(*) filter (where status = 'outage') as outage_buckets_24h,
        count(*) filter (where status = 'outage'
            and bucket_start >= {{ snap }} - interval {{ p['price_window_hours'] }} hour) as outage_buckets_price_window,
        count(*) filter (where bucket_start >= {{ snap }} - interval {{ p['price_window_hours'] }} hour)
            as buckets_recorded_price_window
    from {{ ref('stg_buckets') }}
),

exempt as (
    select item_id
    from {{ ref('tax_exemptions') }}
    where exempt_from <= date '{{ var("last_date") }}'
      and (exempt_to is null or exempt_to > date '{{ var("last_date") }}')
),

regime as (
    select regime_id
    from {{ ref('regimes') }}
    where kind = 'measurement'
      and starts_at <= {{ snap }} - interval 5 minute
      and (ends_at is null or ends_at > {{ snap }} - interval 5 minute)
),

items_seen as (
    select distinct item_id from prices
),

priced as (
    select
        s.item_id,
        i.name,
        i.members,
        i.buy_limit,
        w.instant_buy_vwap,
        w.instant_sell_vwap,
        coalesce(w.instant_buy_volume, 0) as instant_buy_volume_1h,
        coalesce(w.instant_sell_volume, 0) as instant_sell_volume_1h,
        coalesce(e.instant_sell_volume_entry, 0) as instant_sell_volume_entry,
        coalesce(c.two_sided_hours_24h, 0) as two_sided_hours_24h,
        -- Whole coins, rounded against us: buy at or above the reference, sell at or below.
        cast(ceil(w.instant_sell_vwap) as bigint) as buy_price,
        cast(floor(w.instant_buy_vwap) as bigint) as sell_price,
        x.item_id is not null as tax_exempt,
        l.high_time as last_instant_buy_at,
        l.low_time as last_instant_sell_at,
        l.fetched_at as latest_fetched_at,
        date_diff('minute', least(l.high_time, l.low_time), l.fetched_at) as minutes_since_trade
    from items_seen s
    left join {{ ref('stg_items') }} i using (item_id)
    left join price_window w using (item_id)
    left join entry_window e using (item_id)
    left join coverage c using (item_id)
    left join exempt x using (item_id)
    left join {{ ref('stg_latest') }} l using (item_id)
),

taxed as (
    select
        *,
        ge_tax_per_item(sell_price, cast(epoch({{ snap }}) as bigint), tax_exempt) as tax_per_item
    from priced
),

sized as (
    select
        *,
        sell_price - tax_per_item - buy_price as net_margin,
        cast(floor({{ p['capital_gp'] }} / nullif(buy_price, 0)) as bigint) as qty_cap_capital,
        cast(floor({{ p['queue_share'] }} * instant_sell_volume_entry) as bigint) as qty_cap_volume
    from taxed
),

feasible as (
    select
        *,
        least(coalesce(buy_limit, qty_cap_capital), qty_cap_capital, qty_cap_volume) as feasible_quantity
    from sized
),

limited as (
    select
        *,
        case
            when feasible_quantity = qty_cap_volume then 'volume'
            when feasible_quantity = buy_limit then 'buy_limit'
            when feasible_quantity is not null then 'capital'
        end as quantity_limited_by
    from feasible
),

graded as (
    select
        f.*,
        h.buckets_ok_24h,
        h.outage_buckets_24h,
        case
            when f.buy_price is null or f.sell_price is null then 'C'
            when h.buckets_ok_24h + h.outage_buckets_24h < {{ g['min_window_buckets'] }} then 'C'
            when h.buckets_recorded_price_window < {{ p['price_window_hours'] * 12 }} or h.outage_buckets_price_window > 0 then 'C'
            when f.two_sided_hours_24h < {{ g['c_below_two_sided_hours'] }} then 'C'
            when f.minutes_since_trade is not null and f.minutes_since_trade > {{ g['c_above_minutes_since_trade'] }} then 'C'
            when f.two_sided_hours_24h >= {{ g['a_min_two_sided_hours'] }}
                 and f.minutes_since_trade is not null
                 and f.minutes_since_trade <= {{ g['a_max_minutes_since_trade'] }} then 'A'
            else 'B'
        end as data_grade
    from limited f
    cross join window_health h
)

select
    {{ snap }} as snapshot_at,
    date '{{ var("last_date") }}' as snapshot_date,
    '{{ p["id"] }}' as policy_id,
    '{{ g["version"] }}' as data_grade_version,
    (select regime_id from regime) as regime_id,
    item_id,
    name,
    members,
    cast(buy_limit as bigint) as buy_limit,
    {{ snap }} - interval {{ p['price_window_hours'] }} hour as price_window_start,
    {{ snap }} as price_window_end,
    instant_buy_vwap,
    instant_sell_vwap,
    cast(instant_buy_volume_1h as bigint) as instant_buy_volume_1h,
    cast(instant_sell_volume_1h as bigint) as instant_sell_volume_1h,
    cast(instant_sell_volume_entry as bigint) as instant_sell_volume_entry,
    buy_price,
    sell_price,
    tax_exempt,
    cast(tax_per_item as bigint) as tax_per_item,
    cast(net_margin as bigint) as net_margin,
    qty_cap_capital,
    qty_cap_volume,
    cast(feasible_quantity as bigint) as feasible_quantity,
    quantity_limited_by,
    case when net_margin > 0 and feasible_quantity > 0 then cast(net_margin * feasible_quantity as bigint) end as net_gp,
    cast(two_sided_hours_24h as integer) as two_sided_hours_24h,
    cast(buckets_ok_24h as integer) as buckets_ok_24h,
    cast(outage_buckets_24h as integer) as outage_buckets_24h,
    last_instant_buy_at,
    last_instant_sell_at,
    latest_fetched_at,
    cast(minutes_since_trade as bigint) as minutes_since_trade,
    data_grade,
    case
        when buy_price is null or sell_price is null then 'one side did not trade in the last hour'
        when buckets_ok_24h + outage_buckets_24h < {{ g['min_window_buckets'] }} then 'source data incomplete for the last 24 hours'
        when data_grade = 'C' and two_sided_hours_24h < {{ g['c_below_two_sided_hours'] }}
            then 'both sides traded in only ' || two_sided_hours_24h || ' of the last 24 hours'
        when data_grade = 'C' and minutes_since_trade > {{ g['c_above_minutes_since_trade'] }}
            then 'last trade ' || minutes_since_trade || ' minutes before publication'
        when data_grade = 'C' then 'price hour incomplete or hit a source outage'
        when data_grade = 'A'
            then 'both sides traded in all 24 of the last 24 hours, last trade '
                || minutes_since_trade || ' min before publication'
        when two_sided_hours_24h < {{ g['a_min_two_sided_hours'] }}
            then 'both sides traded in ' || two_sided_hours_24h || ' of the last 24 hours (grade A needs '
                || {{ g['a_min_two_sided_hours'] }} || ')'
        when minutes_since_trade is null then 'time of last trade unknown'
        else 'last trade ' || minutes_since_trade || ' min before publication (grade A needs '
            || {{ g['a_max_minutes_since_trade'] }} || ' or fewer)'
    end as data_grade_reason
from graded
