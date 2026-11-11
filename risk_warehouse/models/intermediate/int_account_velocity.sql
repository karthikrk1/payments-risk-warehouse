-- For each account and hour it was active: its activity in the 1, 24 and 168 (7-day) hours
-- *before* that hour. The current hour is excluded (see macros/prior_hours_window.sql), so these
-- can be joined to any transaction in that hour without leaking it, or anything after it.

with activity as (

    select * from {{ ref('int_account_hourly_activity') }}

)

select
    account_id,
    paysim_step,

    coalesce(sum(sent_count + received_count) over ({{ prior_hours_window(1) }}), 0)
        as activity_count_1h,
    coalesce(sum(sent_count + received_count) over ({{ prior_hours_window(24) }}), 0)
        as activity_count_24h,
    coalesce(sum(sent_count + received_count) over ({{ prior_hours_window(168) }}), 0)
        as activity_count_7d,

    coalesce(sum(received_count) over ({{ prior_hours_window(24) }}), 0) as received_count_24h,
    coalesce(sum(received_count) over ({{ prior_hours_window(168) }}), 0) as received_count_7d,
    coalesce(sum(received_amount) over ({{ prior_hours_window(24) }}), 0) as received_amount_24h,
    coalesce(sum(received_amount) over ({{ prior_hours_window(168) }}), 0) as received_amount_7d,

    -- Audit: the latest hour any of these windows read (null if the account had no prior
    -- activity in 7 days). The leakage test checks it is before the transaction.
    max(paysim_step) over ({{ prior_hours_window(168) }}) as latest_source_step

from activity
