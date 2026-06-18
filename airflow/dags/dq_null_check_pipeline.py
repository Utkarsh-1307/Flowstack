"""
DAG 1: dq_null_check_pipeline
Category: Data Quality & Validation

WHAT IT DOES:
  Every hour, checks what percentage of raw_events rows in that time window
  have NULL in user_id or payload. If the null rate exceeds 5%, it writes
  those bad rows into a quarantine table for investigation. Otherwise it
  marks the window as clean.

WHY WE USE IT:
  Production pipelines need automated data quality gates. Without this,
  bad rows silently flow into aggregations and corrupt reports. Catching
  nulls at the source (raw_events) is the cheapest place to fix them.

KEY AIRFLOW CONCEPT TAUGHT:
  BranchPythonOperator — the task returns a task_id STRING (not True/False).
  Airflow uses that string to decide which downstream path to execute, and
  completely skips the other branch. The branches rejoin at dq_complete
  using trigger_rule="none_failed_min_one_success".
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

NULL_RATE_THRESHOLD_PCT = 5.0  # alert if more than 5% rows are null


def _compute_null_stats(**context):
    """
    Connects to Postgres and counts:
      - total rows in the current hourly window
      - rows where user_id IS NULL or payload IS NULL

    Pushes the null percentage to XCom so the branching task can read it.
    XCom (Cross-Communication) is how Airflow tasks share small pieces of
    data with each other — think of it like a shared scratchpad per DAG run.
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE user_id IS NULL OR payload IS NULL) AS nulls
                FROM raw_events
                WHERE created_at >= $1 AND created_at < $2
                """,
                start,
                end,
            )
        finally:
            await conn.close()
        return row

    result = asyncio.run(_query())
    total = result["total"]
    nulls = result["nulls"]
    null_pct = (nulls / total * 100) if total > 0 else 0.0

    print(f"Window {start} → {end}: {total} total rows, {nulls} nulls ({null_pct:.2f}%)")

    # xcom_push stores a value that other tasks can pull with xcom_pull
    context["ti"].xcom_push(key="null_pct", value=null_pct)
    context["ti"].xcom_push(key="total_rows", value=total)
    context["ti"].xcom_push(key="null_rows", value=nulls)


def _branch_null_check(**context):
    """
    BranchPythonOperator callable — MUST return the task_id of the branch to run.
    Airflow will skip every task that is NOT in the returned list.

    If null_pct >= threshold  →  run 'quarantine_records'
    If null_pct < threshold   →  run 'pass_gate'
    """
    null_pct = context["ti"].xcom_pull(task_ids="compute_null_stats", key="null_pct")
    if null_pct is None:
        null_pct = 0.0

    if null_pct >= NULL_RATE_THRESHOLD_PCT:
        print(f"Null rate {null_pct:.2f}% ≥ threshold {NULL_RATE_THRESHOLD_PCT}% — quarantining")
        return "quarantine_records"
    else:
        print(f"Null rate {null_pct:.2f}% < threshold — data is clean")
        return "pass_gate"


def _send_dq_alert(**context):
    """
    Sends a human-readable DQ alert. Re-uses the same requests pattern
    as alerts.py so alerts are consistent across all DAGs.
    """
    import requests

    null_pct = context["ti"].xcom_pull(task_ids="compute_null_stats", key="null_pct")
    total = context["ti"].xcom_pull(task_ids="compute_null_stats", key="total_rows")
    nulls = context["ti"].xcom_pull(task_ids="compute_null_stats", key="null_rows")

    message = (
        f":warning: *Data Quality Alert — Null Check*\n"
        f"*DAG:* `dq_null_check_pipeline`\n"
        f"*Window:* `{context['data_interval_start']}` → `{context['data_interval_end']}`\n"
        f"*Null rate:* `{null_pct:.2f}%` ({nulls}/{total} rows)\n"
        f"*Action:* Rows quarantined in `dq_quarantine` table"
    )

    for url_env in ("SLACK_WEBHOOK_URL", "DISCORD_WEBHOOK_URL"):
        url = os.getenv(url_env, "")
        if url:
            key = "text" if "SLACK" in url_env else "content"
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


with DAG(
    dag_id="dq_null_check_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "validation"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Create quarantine table if it doesn't exist yet ───────────────
    # PostgresOperator runs raw SQL against the 'postgres_default' connection.
    # CREATE TABLE IF NOT EXISTS makes this task safe to re-run at any time.
    # It only does something meaningful on the very first run.
    ensure_quarantine_table = PostgresOperator(
        task_id="ensure_quarantine_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS dq_quarantine (
                id           BIGSERIAL PRIMARY KEY,
                event_id     BIGINT NOT NULL,
                window_start TIMESTAMP WITH TIME ZONE,
                window_end   TIMESTAMP WITH TIME ZONE,
                reason       TEXT NOT NULL,
                quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """,
    )

    # ── Step 2: Compute null statistics for this hour's window ────────────────
    compute_null_stats = PythonOperator(
        task_id="compute_null_stats",
        python_callable=_compute_null_stats,
    )

    # ── Step 3: Branch based on null percentage ───────────────────────────────
    # This is the KEY task. The python_callable MUST return a task_id string.
    # Returning a wrong task_id string causes a runtime error — useful to know.
    branch_on_null_rate = BranchPythonOperator(
        task_id="branch_on_null_rate",
        python_callable=_branch_null_check,
    )

    # ── Branch A: Data is clean — just log and move on ────────────────────────
    pass_gate = EmptyOperator(task_id="pass_gate")

    # ── Branch B: Data has too many nulls — quarantine the bad rows ───────────
    # {{ data_interval_start }} is a Jinja template. Airflow replaces it with
    # the actual datetime value at runtime. This keeps our SQL idempotent.
    quarantine_records = PostgresOperator(
        task_id="quarantine_records",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO dq_quarantine (event_id, window_start, window_end, reason)
            SELECT
                id,
                '{{ data_interval_start }}',
                '{{ data_interval_end }}',
                'null_user_id_or_payload'
            FROM raw_events
            WHERE (user_id IS NULL OR payload IS NULL)
              AND created_at >= '{{ data_interval_start }}'
              AND created_at <  '{{ data_interval_end }}'
            ON CONFLICT DO NOTHING;
        """,
    )

    # ── Step 4: Send DQ failure alert (only runs on the quarantine branch) ────
    notify_dq_failure = PythonOperator(
        task_id="notify_dq_failure",
        python_callable=_send_dq_alert,
    )

    # ── Final: Convergence point — runs after EITHER branch completes ─────────
    # trigger_rule="none_failed_min_one_success" means:
    #   "Run me if at least one upstream succeeded and none failed."
    # This is necessary because when BranchPythonOperator skips a branch,
    # the skipped tasks have state=SKIPPED (not success). Without this
    # trigger_rule, dq_complete would be skipped too (by default Airflow
    # requires ALL upstreams to succeed before running a task).
    dq_complete = EmptyOperator(
        task_id="dq_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # ── Wire up the task dependencies ─────────────────────────────────────────
    ensure_quarantine_table >> compute_null_stats >> branch_on_null_rate
    branch_on_null_rate >> pass_gate >> dq_complete
    branch_on_null_rate >> quarantine_records >> notify_dq_failure >> dq_complete
