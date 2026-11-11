-- Simple rules on the point-in-time features in ml_transaction_features, scored against the label.
-- Not a trained model: the point is to show where the signal is, kept honest by labelling it.
--   real:    from PaySim's own data
--   planted: from the synthetic login/device signal (see the README); proves the pipeline works,
--            not that these signals predict real fraud

{{ rule_metrics(
    'Type is transfer or cash-out',
    "transaction_type in ('transfer', 'cash_out')",
    ref('ml_transaction_features'),
    'real'
) }}

union all

{{ rule_metrics(
    'Transfer/cash-out to a receiver with no activity in the prior 7 days',
    "transaction_type in ('transfer', 'cash_out') and receiver_received_count_7d = 0",
    ref('ml_transaction_features'),
    'real'
) }}

union all

{{ rule_metrics(
    'Transfer to a receiver with no activity in the prior 7 days',
    "transaction_type = 'transfer' and receiver_received_count_7d = 0",
    ref('ml_transaction_features'),
    'real'
) }}

union all

{{ rule_metrics(
    'Failed login in the prior 24 hours',
    'failed_login_count_24h > 0',
    ref('ml_transaction_features'),
    'planted'
) }}

union all

{{ rule_metrics(
    'Device previously used by another account',
    'device_prior_other_account_count > 0',
    ref('ml_transaction_features'),
    'planted'
) }}

union all

{{ rule_metrics(
    'Transfer/cash-out, receiver with no 7-day activity, and a failed login or shared device',
    "transaction_type in ('transfer', 'cash_out') and receiver_received_count_7d = 0 and (failed_login_count_24h > 0 or device_prior_other_account_count > 0)",
    ref('ml_transaction_features'),
    'real + planted'
) }}
