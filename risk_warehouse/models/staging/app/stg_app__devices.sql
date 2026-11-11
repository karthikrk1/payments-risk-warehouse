-- One row per device seen in a login.

with source as (

    select * from {{ source('app', 'app_devices') }}

)

select
    device_id,
    os as device_os,
    first_seen_ts,
    _loaded_at
from source
