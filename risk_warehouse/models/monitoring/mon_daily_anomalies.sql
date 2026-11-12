-- One row per monitored metric per day, scored against its own recent history.
--
-- Baseline: the same metric over the previous `monitoring_baseline_days` days (the day itself
-- excluded), summarised by its median and median absolute deviation (MAD). Both are robust: one bad
-- day doesn't distort the baseline the way it would a mean and standard deviation.
--
-- Score: the modified z-score of Iglewicz & Hoaglin, 0.6745 * (value - median) / MAD; a day is an
-- anomaly when |score| > `monitoring_anomaly_threshold` (3.5, the published rule) and the value is
-- at least `monitoring_min_relative_change` (20%) away from the median. Days covered by
-- a row in the known_events seed are marked `expected` instead: real, explained events in the data.
--
-- Status:
--   insufficient_baseline  fewer than `monitoring_min_baseline_days` days of history
--   ok                     within the threshold (or a flat baseline the day matches)
--   anomaly                outside the threshold and unexplained -> warning test
--   expected               outside the threshold, explained by known_events

{% set baseline_days = var('monitoring_baseline_days') %}
{% set min_baseline_days = var('monitoring_min_baseline_days') %}
{% set threshold = var('monitoring_anomaly_threshold') %}
{% set min_change = var('monitoring_min_relative_change') %}

with metrics as (

    select * from {{ ref('mon_daily_metrics') }}

),

known_events as (

    select * from {{ ref('known_events') }}

),

-- Each day paired with the same metric on each of the previous days.
history as (

    select
        current_day.source,
        current_day.metric_name,
        current_day.dimension,
        current_day.metric_date,
        current_day.metric_value,
        prior_day.metric_value as prior_value
    from metrics as current_day
    left join metrics as prior_day
        on prior_day.source = current_day.source
        and prior_day.metric_name = current_day.metric_name
        and prior_day.dimension = current_day.dimension
        and prior_day.metric_date >= {{ dbt.dateadd('day', -baseline_days, 'current_day.metric_date') }}
        and prior_day.metric_date < current_day.metric_date

),

baseline as (

    select
        source,
        metric_name,
        dimension,
        metric_date,
        metric_value,
        count(prior_value) as baseline_days,
        percentile_cont(0.5) within group (order by prior_value) as baseline_median
    from history
    group by source, metric_name, dimension, metric_date, metric_value

),

spread as (

    select
        baseline.source,
        baseline.metric_name,
        baseline.dimension,
        baseline.metric_date,
        percentile_cont(0.5) within group (
            order by abs(history.prior_value - baseline.baseline_median)
        ) as baseline_mad
    from baseline
    inner join history
        on history.source = baseline.source
        and history.metric_name = baseline.metric_name
        and history.dimension = baseline.dimension
        and history.metric_date = baseline.metric_date
    where history.prior_value is not null
    group by baseline.source, baseline.metric_name, baseline.dimension, baseline.metric_date

),

scored as (

    select
        baseline.source,
        baseline.metric_name,
        baseline.dimension,
        baseline.metric_date,
        baseline.metric_value,
        baseline.baseline_days,
        baseline.baseline_median,
        spread.baseline_mad,
        case
            when spread.baseline_mad > 0
                then 0.6745 * (baseline.metric_value - baseline.baseline_median) / spread.baseline_mad
        end as modified_z_score
    from baseline
    left join spread
        on spread.source = baseline.source
        and spread.metric_name = baseline.metric_name
        and spread.dimension = baseline.dimension
        and spread.metric_date = baseline.metric_date

),

flagged as (

    select
        *,
        case
            when baseline_days < {{ min_baseline_days }} then false
            -- A flat baseline (MAD = 0) has no spread to score against: any change is an anomaly.
            when baseline_mad = 0 then metric_value <> baseline_median
            -- Statistically unusual *and* materially different (see monitoring_min_relative_change).
            else abs(modified_z_score) > {{ threshold }}
                and (
                    baseline_median = 0
                    or abs(metric_value - baseline_median) >= {{ min_change }} * abs(baseline_median)
                )
        end as is_outlier
    from scored

)

select
    flagged.source,
    flagged.metric_name,
    flagged.dimension,
    flagged.metric_date,
    flagged.metric_value,
    flagged.baseline_days,
    flagged.baseline_median,
    flagged.baseline_mad,
    flagged.modified_z_score,
    case
        when flagged.baseline_days < {{ min_baseline_days }} then 'insufficient_baseline'
        when not flagged.is_outlier then 'ok'
        when known_events.event_id is not null then 'expected'
        else 'anomaly'
    end as status,
    known_events.event_id as known_event_id
from flagged
left join known_events
    on known_events.source = flagged.source
    and known_events.metric_name in (flagged.metric_name, '*')
    and flagged.metric_date between known_events.start_date and known_events.end_date
