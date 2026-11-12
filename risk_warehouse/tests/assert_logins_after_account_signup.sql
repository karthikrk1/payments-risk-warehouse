-- An account can't be logged into before it exists. The generator once placed failed attempts up to
-- 6.5 hours before a transaction on accounts opened only 1 hour before it (2,407 such logins). Replay
-- (loading data day by day) would then load a login before its account. Returns offending logins.

select
    logins.login_id,
    logins.login_ts,
    accounts.signup_ts
from {{ ref('stg_app__logins') }} as logins
inner join {{ ref('stg_app__accounts') }} as accounts
    on logins.account_id = accounts.account_id
where logins.login_ts < accounts.signup_ts
