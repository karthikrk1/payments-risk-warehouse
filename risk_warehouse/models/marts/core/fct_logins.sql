-- One row per login attempt, with whether it came from outside the account's home country.

with logins as (

    select * from {{ ref('stg_app__logins') }}

),

accounts as (

    select account_id, country_code from {{ ref('stg_app__accounts') }}

)

select
    logins.login_id,
    {{ dbt_utils.generate_surrogate_key(['logins.account_id']) }} as account_key,
    {{ dbt_utils.generate_surrogate_key(['logins.device_id']) }} as device_key,
    cast(logins.login_ts as date) as login_date,
    logins.login_ts,
    logins.account_id,
    logins.device_id,
    logins.is_success,
    logins.ip_country_code,
    logins.ip_country_code <> accounts.country_code as is_foreign_ip
from logins
left join accounts
    on logins.account_id = accounts.account_id
