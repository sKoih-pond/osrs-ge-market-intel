-- Item attributes in force on the snapshot date (type 2 dimension).
select item_id, name, members, buy_limit, highalch
from {{ source('silver', 'item_dim') }}
where valid_from <= date '{{ var("last_date") }}'
  and (valid_to is null or valid_to > date '{{ var("last_date") }}')
