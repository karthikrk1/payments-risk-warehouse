{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- One row per transaction. Incremental: a run after the first only processes transactions with a
-- higher transaction_id than any already loaded. That is correct because transaction_id is the row
-- number in PaySim's file, which is in time order (step never decreases), so new data always has
-- higher ids. delete+insert on transaction_id makes a re-processed row replace, not duplicate.
-- Rebuild everything with: dbt build --full-refresh --select fct_transactions
--
-- Dimension keys are computed with the same generate_surrogate_key call as the dimensions, so no
-- join is needed here; relationship tests prove every key resolves.

with transactions as (

    select * from {{ ref('stg_paysim__transactions') }}

    {% if is_incremental() %}
    where transaction_id > (select coalesce(max(transaction_id), 0) from {{ this }})
    {% endif %}

)

select
    transaction_id,
    {{ dbt_utils.generate_surrogate_key(['transaction_type']) }} as transaction_type_key,
    {{ dbt_utils.generate_surrogate_key(['sender_account_id']) }} as sender_account_key,
    {{ dbt_utils.generate_surrogate_key(['receiver_account_id']) }} as receiver_account_key,
    cast(transaction_ts as date) as transaction_date,
    transaction_ts,
    paysim_step,
    transaction_type,
    sender_account_id,
    receiver_account_id,
    transaction_amount,
    sender_balance_before_amount,
    sender_balance_after_amount,
    receiver_balance_before_amount,
    receiver_balance_after_amount,
    has_sender_balance_mismatch,
    has_receiver_balance_mismatch,
    has_balance_mismatch,
    is_fraud,
    is_flagged_fraud
from transactions
