-- One row per account: every customer, plus every merchant that received a payment. Merchants
-- have no signup, KYC or country data; including them keeps every fct_transactions receiver key
-- resolvable.

with customers as (

    select
        account_id,
        'customer' as account_type,
        signup_ts,
        kyc_status,
        country_code
    from {{ ref('stg_app__accounts') }}

),

merchants as (

    select distinct
        receiver_account_id as account_id,
        'merchant' as account_type,
        cast(null as {{ dbt.type_timestamp() }}) as signup_ts,
        cast(null as {{ dbt.type_string() }}) as kyc_status,
        cast(null as {{ dbt.type_string() }}) as country_code
    from {{ ref('stg_paysim__transactions') }}
    where is_merchant_receiver

),

accounts as (

    select * from customers
    union all
    select * from merchants

)

select
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_key,
    account_id,
    account_type,
    account_type = 'customer' as is_customer,
    signup_ts,
    cast(signup_ts as date) as signup_date,
    kyc_status,
    country_code
from accounts
