"""Generate synthetic accounts, devices, logins and disputes around the PaySim transactions.

PaySim has transactions only. This script adds the identity, account-access and disputes
domains a payments risk team would also have, keyed to PaySim's own account IDs and
transaction rows, and writes them as Parquet files to data/raw/generated/.

Output is reproducible: every value comes from rnd(key) (see common.register_rnd), so the
same seed always produces identical files.

PLANTED SIGNAL. Logins and devices are partly tied to PaySim's fraud label on purpose:
fraudulent senders more often have failed logins before the transaction and more often use a
device shared with other fraudulent accounts. This lets the feature pipeline be checked end to
end. It is synthetic: features built on logins or devices show that the pipeline works, not that
these signals predict real-world fraud. Everything else (account age, KYC, country, IP country,
device OS) is independent of the label. Disputes follow fraud, as they do in reality, and are
always filed after the transaction they dispute.

Usage:
    python ingestion/generate_entities.py [--seed SEED] [--csv PATH] [--out DIR]
"""

import argparse
import json
import time
from pathlib import Path

import duckdb
from common import (
    DEFAULT_SEED,
    GENERATED_DIR,
    PAYSIM_CSV,
    REPO_ROOT,
    connect,
    paysim_select_sql,
    register_rnd,
    sql_literal,
    transaction_ts_sql,
)

# --- Settings ------------------------------------------------------------------------------

# Accounts: share opened within two days of their first transaction; the rest are 2 days to
# 3 years old at first activity.
NEW_ACCOUNT_SHARE = 0.10
KYC_SHARES = {"verified": 0.85, "pending": 0.10}  # remainder: unverified
HOME_COUNTRY = "US"
HOME_COUNTRY_SHARE = 0.90
OTHER_COUNTRIES = ["CA", "GB", "MX", "IN", "NG", "PH"]
# Logins from an IP outside the account's own country.
FOREIGN_IP_SHARE = 0.03

# Size of the pool of shared devices (public or family devices) legitimate accounts sometimes use.
SHARED_DEVICE_POOL = 50_000

# The only label-dependent settings for logins and devices. See the module docstring.
PLANTED_SIGNAL = {
    # Chance that a transaction is preceded by failed login attempts, and the most attempts.
    "failed_login_rate": {"legit": 0.04, "fraud": 0.30},
    "max_failed_logins": {"legit": 2, "fraud": 4},
    # Chance that the sending device is not the account's own: a shared device for legitimate
    # accounts, or one of a small set of "ring" devices reused across fraudulent accounts.
    "shared_device_rate": {"legit": 0.01, "fraud": 0.30},
    "ring_device_pool": 200,
}

# Chance a transaction is disputed by its sender, by fraud label and type.
DISPUTE_RATE_FRAUD = 0.40
DISPUTE_RATE_LEGIT = {"TRANSFER": 0.002, "CASH_OUT": 0.002, "PAYMENT": 0.0005}
DISPUTE_STATUS_SHARES = {"open": 0.30, "won": 0.45}  # remainder: lost

TABLES = {
    "accounts": "one row per customer account",
    "devices": "one row per device seen in a login",
    "logins": "one row per login attempt",
    "disputes": "one row per disputed transaction",
}


def country_pick_sql(key: str) -> str:
    """SQL that picks one of OTHER_COUNTRIES using rnd(key)."""
    countries = "[" + ", ".join(sql_literal(c) for c in OTHER_COUNTRIES) + "]"
    return f"{countries}[1 + cast(floor(rnd({key}) * {len(OTHER_COUNTRIES)}) as integer)]"


def generate(con: duckdb.DuckDBPyConnection, seed: str, csv_path: Path) -> None:
    """Build the TABLES in `con` from the PaySim CSV."""
    register_rnd(con, seed)
    # Opaque, stable IDs: the kind of device (own, shared, ring) must not be readable from its ID.
    con.execute(
        "create or replace macro device_id(kind, key) as "
        f"'D' || substr(md5({sql_literal(seed)} || '|device|' || kind || '|' || key), 1, 12)"
    )
    con.execute(
        "create or replace macro entity_id(prefix, key) as "
        f"prefix || substr(md5({sql_literal(seed)} || '|' || prefix || '|' || key), 1, 16)"
    )

    # k is the transaction's key for rnd(); every random decision about it starts from k.
    con.execute(f"""
        create or replace table tx as
        select
            source_row_number as transaction_id,
            cast(source_row_number as varchar) as k,
            type,
            nameOrig as sender_id,
            nameDest as receiver_id,
            isFraud = 1 as is_fraud,
            {transaction_ts_sql()} as transaction_ts
        from ({paysim_select_sql(csv_path)})
    """)

    # Accounts: every customer account in PaySim. Merchants (IDs starting with M) only receive
    # and are not customers, so they get no account row.
    verified, pending = KYC_SHARES["verified"], KYC_SHARES["pending"]
    con.execute(f"""
        create or replace table accounts as
        with activity as (
            select sender_id as account_id, transaction_ts from tx
            union all
            select receiver_id, transaction_ts from tx where receiver_id like 'C%'
        ),
        first_activity as (
            select account_id, min(transaction_ts) as first_activity_ts
            from activity
            group by account_id
        )
        select
            account_id,
            case
                when rnd(account_id || '|new') < {NEW_ACCOUNT_SHARE}
                    then first_activity_ts
                        - to_hours(1 + cast(floor(rnd(account_id || '|age') * 47) as integer))
                else first_activity_ts
                    - to_days(2 + cast(floor(rnd(account_id || '|age') * 1094) as integer))
            end as signup_ts,
            case
                when rnd(account_id || '|kyc') < {verified} then 'verified'
                when rnd(account_id || '|kyc') < {verified + pending} then 'pending'
                else 'unverified'
            end as kyc_status,
            case
                when rnd(account_id || '|country') < {HOME_COUNTRY_SHARE}
                    then {sql_literal(HOME_COUNTRY)}
                else {country_pick_sql("account_id || '|country_other'")}
            end as country
        from first_activity
        order by account_id
    """)

    # Per sending transaction: which device it came from and how many failed logins came first.
    # This is where the planted signal is applied (see PLANTED_SIGNAL).
    ps = PLANTED_SIGNAL
    con.execute(f"""
        create or replace table tx_access as
        select
            transaction_id,
            k,
            sender_id,
            transaction_ts,
            case
                when is_fraud and rnd(k || '|shared') < {ps["shared_device_rate"]["fraud"]}
                    then device_id(
                        'ring',
                        cast(floor(rnd(k || '|ring') * {ps["ring_device_pool"]}) as varchar)
                    )
                when not is_fraud and rnd(k || '|shared') < {ps["shared_device_rate"]["legit"]}
                    then device_id(
                        'shared',
                        cast(floor(rnd(k || '|pool') * {SHARED_DEVICE_POOL}) as varchar)
                    )
                else device_id('own', sender_id)
            end as device_id,
            case
                when is_fraud and rnd(k || '|fail') < {ps["failed_login_rate"]["fraud"]}
                    then 1 + cast(
                        floor(rnd(k || '|fail_n') * {ps["max_failed_logins"]["fraud"]}) as integer
                    )
                when not is_fraud and rnd(k || '|fail') < {ps["failed_login_rate"]["legit"]}
                    then 1 + cast(
                        floor(rnd(k || '|fail_n') * {ps["max_failed_logins"]["legit"]}) as integer
                    )
                else 0
            end as failed_logins
        from tx
    """)

    # Logins: one successful login 1-30 minutes before each sending transaction, preceded by
    # its failed attempts 31 minutes to 6.5 hours before it, but never before the account existed:
    # an account can be opened as little as 1 hour before its first transaction, so a failed
    # attempt that would fall before signup is moved to 1 minute after it. (The successful login
    # is always after signup: it's at most 30 minutes before the transaction.) lk is the login's
    # rnd() key.
    max_failed = max(ps["max_failed_logins"].values())
    con.execute(f"""
        create or replace table logins as
        with attempts as (
            select
                k || '|0' as lk,
                sender_id,
                device_id,
                true as is_success,
                transaction_ts
                    - to_minutes(1 + cast(floor(rnd(k || '|ok') * 30) as integer)) as login_ts
            from tx_access
            union all
            select
                k || '|' || cast(attempt.i as varchar) as lk,
                sender_id,
                device_id,
                false as is_success,
                transaction_ts
                    - to_minutes(
                        31 + cast(
                            floor(rnd(k || '|fail_ts|' || cast(attempt.i as varchar)) * 360)
                            as integer
                        )
                    ) as login_ts
            from tx_access
            join range(1, {max_failed + 1}) as attempt(i) on attempt.i <= failed_logins
        )
        select
            entity_id('L', lk) as login_id,
            sender_id as account_id,
            device_id,
            greatest(attempts.login_ts, a.signup_ts + to_minutes(1)) as login_ts,
            is_success,
            case
                when rnd(lk || '|ip') < {1 - FOREIGN_IP_SHARE} then a.country
                else {country_pick_sql("lk || '|ip_other'")}
            end as ip_country
        from attempts
        join accounts as a on a.account_id = attempts.sender_id
        order by login_ts, login_id
    """)

    con.execute("""
        create or replace table devices as
        select
            device_id,
            case
                when rnd(device_id || '|os') < 0.5 then 'ios'
                when rnd(device_id || '|os') < 0.9 then 'android'
                else 'web'
            end as os,
            min(login_ts) as first_seen_ts
        from logins
        group by device_id
        order by device_id
    """)

    # Disputes: filed by the sender 1 hour to 14 days after the transaction. Some are filed
    # after PaySim's 31-day window ends, as late disputes are in reality.
    legit_rate = "\n".join(
        f"when type = {sql_literal(t)} then {rate}" for t, rate in DISPUTE_RATE_LEGIT.items()
    )
    open_share, won_share = DISPUTE_STATUS_SHARES["open"], DISPUTE_STATUS_SHARES["won"]
    con.execute(f"""
        create or replace table disputes as
        select
            entity_id('DSP', k) as dispute_id,
            transaction_id,
            sender_id as account_id,
            transaction_ts
                + to_hours(1 + cast(floor(rnd(k || '|filed') * 336) as integer)) as filed_ts,
            case
                when is_fraud and rnd(k || '|reason') < 0.85 then 'unauthorized'
                when is_fraud then 'not_received'
                when rnd(k || '|reason') < 0.30 then 'unauthorized'
                when rnd(k || '|reason') < 0.80 then 'not_received'
                else 'other'
            end as reason,
            case
                when rnd(k || '|status') < {open_share} then 'open'
                when rnd(k || '|status') < {open_share + won_share} then 'won'
                else 'lost'
            end as status
        from tx
        where rnd(k || '|dispute') < case
            when is_fraud then {DISPUTE_RATE_FRAUD}
            {legit_rate}
            else 0
        end
        order by filed_ts, dispute_id
    """)


def display(path: Path) -> str:
    """A path relative to the repo root when possible, for readable output."""
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--seed", default=DEFAULT_SEED, help="generation seed")
    parser.add_argument("--csv", type=Path, default=PAYSIM_CSV, help="PaySim CSV path")
    parser.add_argument("--out", type=Path, default=GENERATED_DIR, help="output directory")
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"PaySim CSV not found at {args.csv}. See the README Quickstart.")
    args.out.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    con = connect()
    generate(con, args.seed, args.csv)

    counts = {}
    for table in TABLES:
        path = args.out / f"{table}.parquet"
        con.execute(f"copy {table} to {sql_literal(path)} (format parquet)")
        counts[table] = con.execute(f"select count(*) from {table}").fetchone()[0]
        print(f"  {table:<9} {counts[table]:>11,} rows -> {display(path)}")

    manifest = {
        "seed": args.seed,
        "paysim_csv": args.csv.name,
        "row_counts": counts,
        "planted_signal": PLANTED_SIGNAL,
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Generated {len(TABLES)} tables in {time.perf_counter() - started:.1f}s")


if __name__ == "__main__":
    main()
