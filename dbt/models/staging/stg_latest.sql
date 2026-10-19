-- The newest /latest snapshot taken for this hour or the hour before. /latest has no
-- timestamp parameter, so it is a live-state source used only for the staleness input.
with recent as (
    select *
    from {{ source('silver', 'latest_snapshots') }}
    where snapshot_date between date '{{ var("first_date") }}' and date '{{ var("last_date") }}'
      and snapshot_at <= timestamptz '{{ var("snapshot_at") }}'
      and snapshot_at >= timestamptz '{{ var("snapshot_at") }}' - interval 1 hour
)
select item_id, high, high_time, low, low_time, fetched_at, snapshot_at
from recent
qualify snapshot_at = max(snapshot_at) over ()
