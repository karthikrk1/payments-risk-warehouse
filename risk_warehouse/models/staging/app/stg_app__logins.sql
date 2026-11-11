-- One row per login attempt.

with source as (

    select * from {{ source('app', 'app_logins') }}

)

select
    login_id,
    account_id,
    device_id,
    login_ts,
    is_success,
    ip_country as ip_country_code,
    _loaded_at
from source
