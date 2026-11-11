-- Base rates: transactions and fraud by type. Fraud occurs only in transfers and cash-outs.
-- Run: make results (or: cd risk_warehouse && dbt show --select fraud_rate_by_type)

select
    transaction_type,
    count(*) as transaction_count,
    sum(case when is_fraud then 1 else 0 end) as fraud_count,
    round(100.0 * sum(case when is_fraud then 1 else 0 end) / count(*), 3) as fraud_rate_pct,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as share_of_transactions_pct
from {{ ref('fct_transactions') }}
group by transaction_type
order by fraud_count desc, transaction_count desc
