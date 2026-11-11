-- One row per device seen in a login.

select
    {{ dbt_utils.generate_surrogate_key(['device_id']) }} as device_key,
    device_id,
    device_os,
    first_seen_ts,
    cast(first_seen_ts as date) as first_seen_date
from {{ ref('stg_app__devices') }}
