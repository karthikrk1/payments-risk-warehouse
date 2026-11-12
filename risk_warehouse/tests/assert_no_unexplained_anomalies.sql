-- Warning, not error: anomalies are statistical, so they're reported for review rather than blocking
-- the pipeline the way a broken key or a leakage test does. Each returned row is a day where a
-- monitor moved outside its band and no known event explains it.

{{ config(severity='warn') }}

select
    metric_date,
    source,
    metric_name,
    dimension,
    metric_value,
    baseline_median,
    modified_z_score
from {{ ref('mon_daily_anomalies') }}
where status = 'anomaly'
