-- One row per customer account.

with source as (

    select * from {{ source('app', 'app_accounts') }}

)

select
    account_id,
    signup_ts,
    kyc_status,
    country as country_code,
    _loaded_at
from source
