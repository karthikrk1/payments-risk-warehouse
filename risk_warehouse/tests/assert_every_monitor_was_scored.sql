-- Every monitor must actually be scored against real history once there is enough history to score
-- it. This guards against the failure found when evaluating Elementary: a monitor that looks at an
-- empty window reports "no anomaly" and passes without having checked anything.
--
-- "Enough history" = the monitor's source has data on more than `monitoring_min_baseline_days`
-- days. Before that (e.g. the first days of a day-by-day replay) nothing can be scored yet, so nothing
-- is required. After that, a monitor that was never scored, or is missing entirely, is returned.

{% set min_baseline_days = var('monitoring_min_baseline_days') %}

with expected_monitors as (

    select 'transactions' as source, 'row_count' as metric_name
    union all select 'transactions', 'amount_median'
    union all select 'transactions', 'amount_p95'
    union all select 'transactions', 'fraud_share'
    union all select 'transactions', 'null_share'
    union all select 'logins', 'row_count'
    union all select 'logins', 'failed_share'
    union all select 'disputes', 'row_count'
    union all select 'ml_features', 'null_share'

),

source_history as (

    select source, count(distinct metric_date) as days_with_data
    from {{ ref('mon_daily_metrics') }}
    group by source

),

scored as (

    select source, metric_name, count(*) as scored_days
    from {{ ref('mon_daily_anomalies') }}
    where status <> 'insufficient_baseline'
    group by source, metric_name

)

select
    expected_monitors.source,
    expected_monitors.metric_name,
    coalesce(source_history.days_with_data, 0) as days_with_data,
    coalesce(scored.scored_days, 0) as scored_days
from expected_monitors
left join source_history
    on source_history.source = expected_monitors.source
left join scored
    on scored.source = expected_monitors.source
    and scored.metric_name = expected_monitors.metric_name
where coalesce(source_history.days_with_data, 0) > {{ min_baseline_days }}
  and coalesce(scored.scored_days, 0) = 0
