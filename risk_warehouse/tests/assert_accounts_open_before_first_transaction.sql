-- An account must exist before it sends or receives money. Account age at transaction time (a
-- planned feature) would be negative otherwise.

with first_activity as (

    select sender_account_id as account_id, min(transaction_ts) as first_transaction_ts
    from {{ ref('stg_paysim__transactions') }}
    group by sender_account_id

    union all

    select receiver_account_id, min(transaction_ts)
    from {{ ref('stg_paysim__transactions') }}
    where not is_merchant_receiver
    group by receiver_account_id

)

select
    a.account_id,
    a.signup_ts,
    f.first_transaction_ts
from {{ ref('stg_app__accounts') }} as a
inner join first_activity as f
    on a.account_id = f.account_id
where a.signup_ts > f.first_transaction_ts
