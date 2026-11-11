-- Independent check of the window-function velocity: recount, by brute-force self-join, how many
-- transactions each customer receiver had received in the 24 and 168 hours before the current
-- hour, and compare with ml_transaction_features. A wrong RANGE frame (including the current hour,
-- or an off-by-one) shows up here. Returns mismatching rows.

with received as (

    select transaction_id, receiver_account_id, paysim_step
    from {{ ref('stg_paysim__transactions') }}
    where not is_merchant_receiver

),

recount as (

    select
        current_tx.transaction_id,
        sum(case when prior_tx.paysim_step >= current_tx.paysim_step - 24 then 1 else 0 end)
            as received_count_24h,
        count(prior_tx.transaction_id) as received_count_7d
    from received as current_tx
    left join received as prior_tx
        on prior_tx.receiver_account_id = current_tx.receiver_account_id
        and prior_tx.paysim_step between current_tx.paysim_step - 168 and current_tx.paysim_step - 1
    group by current_tx.transaction_id

)

select
    features.transaction_id,
    features.receiver_received_count_24h,
    recount.received_count_24h as recounted_24h,
    features.receiver_received_count_7d,
    recount.received_count_7d as recounted_7d
from {{ ref('ml_transaction_features') }} as features
inner join recount
    on features.transaction_id = recount.transaction_id
where features.receiver_received_count_24h <> recount.received_count_24h
   or features.receiver_received_count_7d <> recount.received_count_7d
