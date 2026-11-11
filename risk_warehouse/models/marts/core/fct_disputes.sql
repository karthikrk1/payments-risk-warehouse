-- One row per dispute, with how long after the transaction it was filed.

with disputes as (

    select * from {{ ref('stg_app__disputes') }}

),

transactions as (

    select transaction_id, transaction_ts from {{ ref('stg_paysim__transactions') }}

)

select
    disputes.dispute_id,
    disputes.transaction_id,
    {{ dbt_utils.generate_surrogate_key(['disputes.account_id']) }} as account_key,
    cast(disputes.filed_ts as date) as filed_date,
    disputes.filed_ts,
    disputes.account_id,
    disputes.dispute_reason,
    disputes.dispute_status,
    {{ dbt.datediff('transactions.transaction_ts', 'disputes.filed_ts', 'hour') }} as hours_to_file
from disputes
left join transactions
    on disputes.transaction_id = transactions.transaction_id
