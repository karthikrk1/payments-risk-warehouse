-- Why the balance columns are excluded from features: rules built on them look almost perfect,
-- but they use the balance *after* the transaction, which a real-time check wouldn't have, and the
-- dataset's author says balances are distorted for fraud (fraud was cancelled in the simulation).
-- A model trained on them would learn the simulation, not fraud.

{{ rule_metrics(
    'LEAK: sender account emptied exactly (after = 0, before = amount)',
    "transaction_type in ('transfer', 'cash_out') and sender_balance_after_amount = 0 and sender_balance_before_amount = transaction_amount",
    ref('fct_transactions'),
    'leak'
) }}

union all

{{ rule_metrics(
    'LEAK: sender balance adds up (no mismatch), transfer or cash-out',
    "transaction_type in ('transfer', 'cash_out') and not has_sender_balance_mismatch",
    ref('fct_transactions'),
    'leak'
) }}
