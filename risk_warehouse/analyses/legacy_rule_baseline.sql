-- The baseline: PaySim's legacy rule (isFlaggedFraud) scored against the label, next to the
-- rule Kaggle documents for it ("transfers over 200.000"), which doesn't reproduce the flag.

{{ rule_metrics(
    'Legacy flag (isFlaggedFraud)',
    'is_flagged_fraud',
    ref('fct_transactions'),
    'baseline'
) }}

union all

{{ rule_metrics(
    'Documented rule: transfer over 200,000',
    "transaction_type = 'transfer' and transaction_amount > 200000",
    ref('fct_transactions'),
    'baseline'
) }}
