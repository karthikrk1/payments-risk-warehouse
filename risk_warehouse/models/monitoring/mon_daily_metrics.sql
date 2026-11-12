-- One row per monitored metric per day (and per dimension where it applies), computed from the data
-- itself. Because nothing here depends on when the pipeline ran, a day-by-day replay produces the
-- same metrics as a one-shot build. (What each run *saw* at the time is logged separately, in
-- ops.run_log, by the Prefect flow.)
--
-- Volume metrics cover every day from a source's first to its last day, so a day with no rows is
-- measured as 0 rather than silently missing.

with transactions as (

    select * from {{ ref('fct_transactions') }}

),

logins as (

    select * from {{ ref('fct_logins') }}

),

disputes as (

    select * from {{ ref('fct_disputes') }}

),

features as (

    select * from {{ ref('ml_transaction_features') }}

),

days as (

    select date_day from {{ ref('dim_date') }}

),

daily_counts as (

    select 'transactions' as source, transaction_date as metric_date, count(*) as row_count
    from transactions
    group by transaction_date

    union all

    select 'logins', login_date, count(*)
    from logins
    group by login_date

    union all

    select 'disputes', filed_date, count(*)
    from disputes
    group by filed_date

),

source_ranges as (

    select source, min(metric_date) as first_date, max(metric_date) as last_date
    from daily_counts
    group by source

),

volume as (

    select
        source_ranges.source,
        days.date_day as metric_date,
        'row_count' as metric_name,
        'all' as dimension,
        cast(coalesce(daily_counts.row_count, 0) as double) as metric_value
    from source_ranges
    inner join days
        on days.date_day between source_ranges.first_date and source_ranges.last_date
    left join daily_counts
        on daily_counts.source = source_ranges.source
        and daily_counts.metric_date = days.date_day

),

-- Distribution of amounts per transaction type. percentile_cont ... within group is standard SQL
-- (DuckDB and Snowflake).
amounts as (

    select
        'transactions' as source,
        transaction_date as metric_date,
        'amount_median' as metric_name,
        transaction_type as dimension,
        cast(percentile_cont(0.5) within group (order by transaction_amount) as double) as metric_value
    from transactions
    group by transaction_date, transaction_type

    union all

    select
        'transactions',
        transaction_date,
        'amount_p95',
        transaction_type,
        cast(percentile_cont(0.95) within group (order by transaction_amount) as double)
    from transactions
    group by transaction_date, transaction_type

),

shares as (

    -- Share of each type's transactions labelled fraud.
    select
        'transactions' as source,
        transaction_date as metric_date,
        'fraud_share' as metric_name,
        transaction_type as dimension,
        avg(case when is_fraud then 1.0 else 0.0 end) as metric_value
    from transactions
    group by transaction_date, transaction_type

    union all

    -- Receiver balance is null by design for merchants; a jump in its null share means the mix of
    -- payments changed or merchant handling broke.
    select
        'transactions',
        transaction_date,
        'null_share',
        'receiver_balance_before_amount',
        avg(case when receiver_balance_before_amount is null then 1.0 else 0.0 end)
    from transactions
    group by transaction_date

    union all

    select
        'logins',
        login_date,
        'failed_share',
        'all',
        avg(case when is_success then 0.0 else 1.0 end)
    from logins
    group by login_date

    union all

    -- Null when the receiver had no activity in the prior week (or is a merchant).
    select
        'ml_features',
        cast(as_of_ts as date),
        'null_share',
        'amount_to_receiver_avg_7d_ratio',
        avg(case when amount_to_receiver_avg_7d_ratio is null then 1.0 else 0.0 end)
    from features
    group by cast(as_of_ts as date)

)

select source, metric_date, metric_name, dimension, metric_value from volume
union all
select source, metric_date, metric_name, dimension, metric_value from amounts
union all
select source, metric_date, metric_name, dimension, metric_value from shares
