-- One row per PaySim transaction: renamed, typed and with balance-consistency flags.
-- Staging rules: no joins, no business logic beyond cleaning.

with source as (

    select * from {{ source('paysim', 'paysim_transactions') }}

),

renamed as (

    select
        source_row_number as transaction_id,
        step as paysim_step,
        -- dbt.dateadd compiles to each warehouse's own date function, so this ports to Snowflake.
        {{ dbt.dateadd(
            'hour',
            'step - 1',
            "cast('" ~ var('paysim_start_date') ~ "' as timestamp)"
        ) }} as transaction_ts,
        lower(type) as transaction_type,
        cast(amount as decimal(18, 2)) as transaction_amount,

        nameOrig as sender_account_id,
        nameDest as receiver_account_id,
        nameDest like 'M%' as is_merchant_receiver,

        cast(oldbalanceOrg as decimal(18, 2)) as sender_balance_before_amount,
        cast(newbalanceOrig as decimal(18, 2)) as sender_balance_after_amount,
        -- Merchants carry no balance information: PaySim's 0 means unknown, so it becomes null.
        case
            when nameDest like 'M%' then null
            else cast(oldbalanceDest as decimal(18, 2))
        end as receiver_balance_before_amount,
        case
            when nameDest like 'M%' then null
            else cast(newbalanceDest as decimal(18, 2))
        end as receiver_balance_after_amount,

        isFraud = 1 as is_fraud,
        isFlaggedFraud = 1 as is_flagged_fraud,
        _loaded_at

    from source

),

flagged as (

    select
        *,
        -- Money leaves the sender on every type except cash_in, where it arrives.
        abs(
            sender_balance_after_amount
            - case
                when transaction_type = 'cash_in'
                    then sender_balance_before_amount + transaction_amount
                else sender_balance_before_amount - transaction_amount
            end
        ) > 0.01 as has_sender_balance_mismatch,
        -- The receiver gains the amount, except on cash_in, where the receiving agent pays out.
        -- Null for merchants, whose balances are unknown.
        case
            when is_merchant_receiver then null
            else abs(
                receiver_balance_after_amount
                - case
                    when transaction_type = 'cash_in'
                        then receiver_balance_before_amount - transaction_amount
                    else receiver_balance_before_amount + transaction_amount
                end
            ) > 0.01
        end as has_receiver_balance_mismatch

    from renamed

)

select
    transaction_id,
    paysim_step,
    transaction_ts,
    transaction_type,
    transaction_amount,
    sender_account_id,
    receiver_account_id,
    is_merchant_receiver,
    sender_balance_before_amount,
    sender_balance_after_amount,
    receiver_balance_before_amount,
    receiver_balance_after_amount,
    has_sender_balance_mismatch,
    has_receiver_balance_mismatch,
    -- Either side; a merchant receiver (null) counts as no mismatch on that side.
    has_sender_balance_mismatch or coalesce(has_receiver_balance_mismatch, false)
        as has_balance_mismatch,
    is_fraud,
    is_flagged_fraud,
    _loaded_at
from flagged
