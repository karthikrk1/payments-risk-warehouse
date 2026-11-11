-- Every transaction falls inside PaySim's 31-day window starting at paysim_start_date.
-- Catches a wrong anchor date or a step outside the expected range.

{% set window_start = "cast('" ~ var('paysim_start_date') ~ "' as timestamp)" %}

select
    transaction_id,
    transaction_ts
from {{ ref('stg_paysim__transactions') }}
where transaction_ts < {{ window_start }}
   or transaction_ts >= {{ dbt.dateadd('day', 31, window_start) }}
