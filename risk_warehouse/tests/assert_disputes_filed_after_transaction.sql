-- A dispute can only be filed after the transaction it disputes, and only by its sender.
-- Point-in-time features rely on this ordering.

select
    d.dispute_id,
    d.filed_ts,
    t.transaction_ts,
    d.account_id,
    t.sender_account_id
from {{ ref('stg_app__disputes') }} as d
inner join {{ ref('stg_paysim__transactions') }} as t
    on d.transaction_id = t.transaction_id
where d.filed_ts <= t.transaction_ts
   or d.account_id <> t.sender_account_id
