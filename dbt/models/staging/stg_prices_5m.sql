-- Trailing 24 hours of 5-minute buckets before the snapshot; no look-ahead (ADR-0004).
select
    bucket_start,
    item_id,
    avg_high_price,
    high_volume,
    avg_low_price,
    low_volume,
    regime_id
from {{ source('silver', 'prices_5m') }}
where bucket_date between date '{{ var("first_date") }}' and date '{{ var("last_date") }}'
  and bucket_start >= timestamptz '{{ var("window_start") }}'
  and bucket_start < timestamptz '{{ var("snapshot_at") }}'
