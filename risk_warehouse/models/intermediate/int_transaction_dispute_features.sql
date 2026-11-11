-- Per transaction: disputes already filed before it. Disputes follow fraud, so they become known
-- only after the fact; counting only those with filed_ts < transaction_ts is what keeps this
-- point-in-time correct. Two views: disputes the sender had filed, and disputes on transactions
-- the receiver had received (a mule account collects disputed transfers).

with transactions as (

    select transaction_id, sender_account_id, receiver_account_id, transaction_ts
    from {{ ref('stg_paysim__transactions') }}

),

disputes as (

    select
        disputes.dispute_id,
        disputes.account_id as filed_by_account_id,
        disputed.receiver_account_id as disputed_receiver_account_id,
        disputes.filed_ts
    from {{ ref('stg_app__disputes') }} as disputes
    inner join transactions as disputed
        on disputes.transaction_id = disputed.transaction_id

),

sender_history as (

    select
        transactions.transaction_id,
        count(*) as sender_prior_dispute_count,
        max(disputes.filed_ts) as latest_filed_ts
    from transactions
    inner join disputes
        on disputes.filed_by_account_id = transactions.sender_account_id
        and disputes.filed_ts < transactions.transaction_ts
    group by transactions.transaction_id

),

receiver_history as (

    select
        transactions.transaction_id,
        count(*) as receiver_prior_dispute_count,
        max(disputes.filed_ts) as latest_filed_ts
    from transactions
    inner join disputes
        on disputes.disputed_receiver_account_id = transactions.receiver_account_id
        and disputes.filed_ts < transactions.transaction_ts
    group by transactions.transaction_id

)

select
    transactions.transaction_id,
    coalesce(sender_history.sender_prior_dispute_count, 0) as sender_prior_dispute_count,
    coalesce(receiver_history.receiver_prior_dispute_count, 0) as receiver_prior_dispute_count,
    -- Audit: the latest dispute either count read. Null-safe greatest, as in
    -- int_transaction_login_features.
    greatest(
        coalesce(sender_history.latest_filed_ts, receiver_history.latest_filed_ts),
        coalesce(receiver_history.latest_filed_ts, sender_history.latest_filed_ts)
    ) as latest_source_ts
from transactions
left join sender_history
    on transactions.transaction_id = sender_history.transaction_id
left join receiver_history
    on transactions.transaction_id = receiver_history.transaction_id
