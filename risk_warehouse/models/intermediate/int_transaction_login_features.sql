-- Per transaction: the sender's login behaviour before it, and how widely the device of their
-- latest successful login had been shared. Everything is limited to logins strictly before
-- transaction_ts. Failed logins and shared devices carry a planted, synthetic fraud signal.

with transactions as (

    select transaction_id, sender_account_id, transaction_ts
    from {{ ref('stg_paysim__transactions') }}

),

logins as (

    select * from {{ ref('stg_app__logins') }}

),

accounts as (

    select account_id, country_code from {{ ref('stg_app__accounts') }}

),

-- The sender's logins in the 24 hours before the transaction.
prior_logins as (

    select
        transactions.transaction_id,
        logins.login_ts,
        logins.is_success,
        logins.device_id,
        logins.ip_country_code
    from transactions
    inner join logins
        on logins.account_id = transactions.sender_account_id
        and logins.login_ts < transactions.transaction_ts
        and logins.login_ts >= {{ dbt.dateadd('hour', -24, 'transactions.transaction_ts') }}

),

login_counts as (

    select
        transaction_id,
        count(*) as login_count_24h,
        sum(case when is_success then 0 else 1 end) as failed_login_count_24h,
        max(login_ts) as latest_login_ts
    from prior_logins
    group by transaction_id

),

latest_successful_login as (

    select transaction_id, device_id, ip_country_code
    from (
        select
            *,
            row_number() over (partition by transaction_id order by login_ts desc) as recency_rank
        from prior_logins
        where is_success
    ) as ranked
    where recency_rank = 1

),

-- Other accounts that had logged in on that device before this transaction.
device_sharing as (

    select
        latest.transaction_id,
        count(distinct logins.account_id) as device_prior_other_account_count,
        max(logins.login_ts) as latest_device_login_ts
    from latest_successful_login as latest
    inner join transactions
        on latest.transaction_id = transactions.transaction_id
    inner join logins
        on logins.device_id = latest.device_id
        and logins.login_ts < transactions.transaction_ts
        and logins.account_id <> transactions.sender_account_id
    group by latest.transaction_id

)

select
    transactions.transaction_id,
    coalesce(login_counts.login_count_24h, 0) as login_count_24h,
    coalesce(login_counts.failed_login_count_24h, 0) as failed_login_count_24h,
    coalesce(device_sharing.device_prior_other_account_count, 0)
        as device_prior_other_account_count,
    latest.ip_country_code <> accounts.country_code as is_latest_login_foreign_ip,
    -- Audit: the latest login any of these features read. Null-safe greatest: Snowflake's
    -- greatest() returns null if any argument is null, DuckDB's ignores nulls; the coalesce pairs
    -- give the same answer on both.
    greatest(
        coalesce(login_counts.latest_login_ts, device_sharing.latest_device_login_ts),
        coalesce(device_sharing.latest_device_login_ts, login_counts.latest_login_ts)
    ) as latest_source_ts
from transactions
left join login_counts
    on transactions.transaction_id = login_counts.transaction_id
left join latest_successful_login as latest
    on transactions.transaction_id = latest.transaction_id
left join device_sharing
    on transactions.transaction_id = device_sharing.transaction_id
left join accounts
    on transactions.sender_account_id = accounts.account_id
