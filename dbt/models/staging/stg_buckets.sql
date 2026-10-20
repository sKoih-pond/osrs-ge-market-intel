-- Manifest rows for the same 24-hour window: which buckets are recorded, and which were outages.
select bucket_start, item_count, status
from {{ source('silver', 'wiki_5m_buckets') }}
where bucket_start >= timestamptz '{{ var("window_start") }}'
  and bucket_start < timestamptz '{{ var("snapshot_at") }}'
