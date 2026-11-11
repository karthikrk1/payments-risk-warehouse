-- One row per customer account per PaySim hour (step) in which it sent or received money.
-- Merchants are excluded: they only receive payments and are never fraud receivers.

with transactions as (

    select * from {{ ref('stg_paysim__transactions') }}

),

sent as (

    select
        sender_account_id as account_id,
        paysim_step,
        count(*) as sent_count,
        sum(transaction_amount) as sent_amount
    from transactions
    group by sender_account_id, paysim_step

),

received as (

    select
        receiver_account_id as account_id,
        paysim_step,
        count(*) as received_count,
        sum(transaction_amount) as received_amount
    from transactions
    where not is_merchant_receiver
    group by receiver_account_id, paysim_step

)

select
    coalesce(sent.account_id, received.account_id) as account_id,
    coalesce(sent.paysim_step, received.paysim_step) as paysim_step,
    coalesce(sent.sent_count, 0) as sent_count,
    coalesce(sent.sent_amount, 0) as sent_amount,
    coalesce(received.received_count, 0) as received_count,
    coalesce(received.received_amount, 0) as received_amount
from sent
full outer join received
    on sent.account_id = received.account_id
    and sent.paysim_step = received.paysim_step
