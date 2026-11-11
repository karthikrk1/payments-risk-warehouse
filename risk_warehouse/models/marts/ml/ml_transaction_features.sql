-- One row per transaction: point-in-time-correct features, the label, the legacy rule as a
-- baseline, and audit columns that the leakage tests check.
--
-- Rules (enforced by tests in tests/):
--   * every feature uses only data from before the transaction's hour;
--   * no balance column, and nothing derived from one, is a feature (the dataset's author says
--     balances must not be used for fraud detection: fraud was cancelled in the simulation).

with transactions as (

    select * from {{ ref('stg_paysim__transactions') }}

),

accounts as (

    select account_id, signup_ts, kyc_status from {{ ref('stg_app__accounts') }}

),

velocity as (

    select * from {{ ref('int_account_velocity') }}

),

logins as (

    select * from {{ ref('int_transaction_login_features') }}

),

disputes as (

    select * from {{ ref('int_transaction_dispute_features') }}

)

select
    -- Identity and time
    transactions.transaction_id,
    transactions.transaction_ts as as_of_ts,

    -- Transaction (known at decision time)
    transactions.transaction_type,
    transactions.transaction_amount,
    mod(transactions.paysim_step - 1, 24) as hour_of_day,
    transactions.is_merchant_receiver,

    -- Accounts. KYC status is a current snapshot (no history in the source); in production it
    -- should be the status as of the transaction (a type 2 slowly changing dimension).
    {{ dbt.datediff('sender.signup_ts', 'transactions.transaction_ts', 'hour') }}
        as sender_account_age_hours,
    {{ dbt.datediff('receiver.signup_ts', 'transactions.transaction_ts', 'hour') }}
        as receiver_account_age_hours,
    sender.kyc_status as sender_kyc_status,

    -- Velocity in the hours before this one. Sender history is almost always empty (99.85% of
    -- PaySim senders appear once), so the receiver side carries the signal.
    sender_velocity.activity_count_1h as sender_activity_count_1h,
    sender_velocity.activity_count_24h as sender_activity_count_24h,
    sender_velocity.activity_count_7d as sender_activity_count_7d,
    receiver_velocity.received_count_24h as receiver_received_count_24h,
    receiver_velocity.received_count_7d as receiver_received_count_7d,
    receiver_velocity.received_amount_24h as receiver_received_amount_24h,
    receiver_velocity.received_amount_7d as receiver_received_amount_7d,
    transactions.transaction_amount
        / nullif(receiver_velocity.received_amount_7d / nullif(receiver_velocity.received_count_7d, 0), 0)
        as amount_to_receiver_avg_7d_ratio,

    -- Logins and devices (planted, synthetic signal; see the README)
    logins.login_count_24h,
    logins.failed_login_count_24h,
    logins.device_prior_other_account_count,
    logins.is_latest_login_foreign_ip,

    -- Disputes already filed
    disputes.sender_prior_dispute_count,
    disputes.receiver_prior_dispute_count,

    -- Label and baseline (not features)
    transactions.is_fraud,
    transactions.is_flagged_fraud as baseline_is_flagged_fraud,

    -- Audit: the latest source event each feature group read. Must all be before as_of_ts.
    sender.signup_ts as audit_sender_signup_ts,
    receiver.signup_ts as audit_receiver_signup_ts,
    {{ dbt.dateadd(
        'hour',
        'sender_velocity.latest_source_step - 1',
        "cast('" ~ var('paysim_start_date') ~ "' as timestamp)"
    ) }} as audit_sender_velocity_ts,
    {{ dbt.dateadd(
        'hour',
        'receiver_velocity.latest_source_step - 1',
        "cast('" ~ var('paysim_start_date') ~ "' as timestamp)"
    ) }} as audit_receiver_velocity_ts,
    logins.latest_source_ts as audit_login_ts,
    disputes.latest_source_ts as audit_dispute_ts

from transactions
left join accounts as sender
    on transactions.sender_account_id = sender.account_id
left join accounts as receiver
    on transactions.receiver_account_id = receiver.account_id
left join velocity as sender_velocity
    on transactions.sender_account_id = sender_velocity.account_id
    and transactions.paysim_step = sender_velocity.paysim_step
left join velocity as receiver_velocity
    on transactions.receiver_account_id = receiver_velocity.account_id
    and transactions.paysim_step = receiver_velocity.paysim_step
left join logins
    on transactions.transaction_id = logins.transaction_id
left join disputes
    on transactions.transaction_id = disputes.transaction_id
