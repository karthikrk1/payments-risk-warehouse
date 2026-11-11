-- The leakage test. Every feature group in ml_transaction_features records the latest source event
-- it read (audit_* columns). All of them must be strictly before the transaction's own time
-- (as_of_ts). Velocity windows end at the previous hour, so their audit time must also be at least
-- an hour earlier. Returns the offending rows; the test passes on 0.

select
    transaction_id,
    as_of_ts,
    audit_sender_signup_ts,
    audit_receiver_signup_ts,
    audit_sender_velocity_ts,
    audit_receiver_velocity_ts,
    audit_login_ts,
    audit_dispute_ts
from {{ ref('ml_transaction_features') }}
where audit_sender_signup_ts >= as_of_ts
   or audit_receiver_signup_ts >= as_of_ts
   or audit_sender_velocity_ts > {{ dbt.dateadd('hour', -1, 'as_of_ts') }}
   or audit_receiver_velocity_ts > {{ dbt.dateadd('hour', -1, 'as_of_ts') }}
   or audit_login_ts >= as_of_ts
   or audit_dispute_ts >= as_of_ts
