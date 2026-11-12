"""Evaluation cases: realistic breakages planted in a copy of the project, with known answers.

Each case changes one thing (a code edit, or a change to the raw data after loading) in a scratch
copy of the dbt project built on the committed CI sample. The runner checks that the change really
makes the build fail, then asks the triage agent to diagnose it and grades the answer:
  - the category must match exactly;
  - every `must_mention` group must be satisfied: at least one of its terms appears (ignoring case)
    in the diagnosis's summary, root cause, suspect files or suggested fix. Groups name the cause,
    not the symptom, so a diagnosis that only restates the failing test does not pass.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Edit:
    """Replace `old` with `new` in a project file (path relative to risk_warehouse/)."""

    path: str
    old: str
    new: str


@dataclass(frozen=True)
class Case:
    name: str
    description: str
    expected_category: str
    must_mention: list[list[str]]
    edits: list[Edit] = field(default_factory=list)
    raw_sql: list[str] = field(default_factory=list)  # run against the raw data after loading


CASES = [
    Case(
        name="window_includes_current_hour",
        description="The shared window macro's frame now ends at the current row, not 1 preceding.",
        expected_category="code_bug",
        must_mention=[["prior_hours_window"], ["current row", "1 preceding"]],
        edits=[
            Edit(
                "macros/prior_hours_window.sql",
                "and 1 preceding",
                "and current row",
            )
        ],
    ),
    Case(
        name="balance_feature_added",
        description="A balance column was added to the ML features (and to its contract).",
        expected_category="code_bug",
        must_mention=[["sender_balance_before_amount"], ["balance"]],
        edits=[
            Edit(
                "models/marts/ml/ml_transaction_features.sql",
                "    transactions.transaction_amount,\n    mod(",
                "    transactions.transaction_amount,\n"
                "    transactions.sender_balance_before_amount,\n    mod(",
            ),
            Edit(
                "models/marts/ml/_ml__models.yml",
                "        description: Amount, known at decision time.\n",
                "        description: Amount, known at decision time.\n"
                "      - name: sender_balance_before_amount\n"
                "        data_type: decimal(18,2)\n"
                "        description: Sender balance before the transaction.\n",
            ),
        ],
    ),
    Case(
        name="contract_cast_removed",
        description="A cast was removed: a SUM widens to DECIMAL(38,2) and breaks the contract.",
        expected_category="code_bug",
        must_mention=[["receiver_received_amount_7d"], ["cast", "decimal(18"]],
        edits=[
            Edit(
                "models/marts/ml/ml_transaction_features.sql",
                "cast(receiver_velocity.received_amount_7d as decimal(18, 2)) "
                "as receiver_received_amount_7d",
                "receiver_velocity.received_amount_7d as receiver_received_amount_7d",
            )
        ],
    ),
    Case(
        name="fraud_flag_inverted",
        description="Staging maps isFraud = 0 to is_fraud, inverting the label.",
        expected_category="code_bug",
        must_mention=[["stg_paysim__transactions"], ["isfraud = 0", "isfraud = 1", "invert"]],
        edits=[
            Edit(
                "models/staging/paysim/stg_paysim__transactions.sql",
                "isFraud = 1 as is_fraud",
                "isFraud = 0 as is_fraud",
            )
        ],
    ),
    Case(
        name="failed_share_monitor_removed",
        description="The logins failed_share monitor was deleted from mon_daily_metrics.",
        expected_category="code_bug",
        must_mention=[["failed_share"], ["mon_daily_metrics"]],
        edits=[
            Edit(
                "models/monitoring/mon_daily_metrics.sql",
                "    select\n        'logins',\n        login_date,\n        'failed_share',\n"
                "        'all',\n        avg(case when is_success then 0.0 else 1.0 end)\n"
                "    from logins\n    group by login_date\n\n    union all\n\n",
                "",
            )
        ],
    ),
    Case(
        name="duplicate_raw_transaction",
        description="One PaySim row was loaded twice.",
        expected_category="data_problem",
        must_mention=[["duplicate", "twice"], ["raw.paysim_transactions", "source_row_number"]],
        raw_sql=[
            "insert into raw.paysim_transactions "
            "select * from raw.paysim_transactions where source_row_number = 1200"
        ],
    ),
    Case(
        name="login_before_signup",
        description="Five logins in the raw data are dated years before their account existed.",
        expected_category="data_problem",
        must_mention=[["app_logins", "raw logins", "login data", "logins table"], ["signup"]],
        raw_sql=[
            "update raw.app_logins set login_ts = login_ts - interval 3 years "
            "where login_id in (select login_id from raw.app_logins order by login_id limit 5)"
        ],
    ),
    Case(
        name="window_test_off_by_one_day",
        description="The PaySim window test now allows 30 days instead of PaySim's 31.",
        expected_category="test_problem",
        must_mention=[["31"], ["assert_transaction_ts_within_paysim_window"]],
        edits=[
            Edit(
                "tests/assert_transaction_ts_within_paysim_window.sql",
                "dbt.dateadd('day', 31, window_start)",
                "dbt.dateadd('day', 30, window_start)",
            )
        ],
    ),
    Case(
        name="disputes_table_missing",
        description="The raw disputes table was never loaded.",
        expected_category="environment",
        must_mention=[["app_disputes"], ["load_raw", "make load", "make ingest", "ingest", "load"]],
        raw_sql=["drop table raw.app_disputes"],
    ),
]
