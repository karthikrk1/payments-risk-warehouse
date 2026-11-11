-- One row per disputed transaction.

with source as (

    select * from {{ source('app', 'app_disputes') }}

)

select
    dispute_id,
    transaction_id,
    account_id,
    filed_ts,
    reason as dispute_reason,
    status as dispute_status,
    _loaded_at
from source
