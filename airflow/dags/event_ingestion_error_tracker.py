"""
DAG 21: event_ingestion_error_tracker
Category: DLQ & Error Handling

WHAT IT DOES:
  Every 2 hours, creates the ingestion_errors and error_classification_summary
  tables (if they don't exist), aggregates errors from the current window,
  classifies them by type, and sends a critical alert if auth errors appear
  or total error count exceeds 1000.

WHY WE USE IT:
  FastAPI validation errors at the ingestion boundary are often the first
  sign of a misbehaving upstream client. Catching them early (not waiting
  for a missing-data complaint) lets us fix the source before it impacts
  downstream aggregations.

KEY AIRFLOW CONCEPT TAUGHT:
  PostgresOperator for table bootstrapping — using CREATE TABLE IF NOT EXISTS
  as the very first task in a DAG is a reliable pattern for self-contained
  DAGs that own their schema. The DAG creates what it needs on first run.
  Also shows BranchPythonOperator returning different task_ids based on
  XCom data pulled from a preceding task.
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

CRITICAL_AUTH_ERRORS = 1        # any auth error is immediately critical
CRITICAL_TOTAL_ERRORS = 1000    # > 1000 errors in a 2-hour window is critical


def _aggregate_and_classify(**context):
    """
    Reads the ingestion_errors table for the current window, counts by type,
    and pushes the results to XCom.

    In a real system, the FastAPI gateway would INSERT into ingestion_errors
    whenever a request fails schema validation. This DAG processes that table.
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            rows = await conn.fetch(
                """
                SELECT
                    error_type,
                    COUNT(*) AS cnt
                FROM ingestion_errors
                WHERE occurred_at >= $1 AND occurred_at < $2
                GROUP BY error_type
                ORDER BY cnt DESC
                """,
                start,
                end,
            )
        finally:
            await conn.close()
        return rows

    rows = asyncio.run(_query())

    by_type = {r["error_type"]: r["cnt"] for r in rows}
    total = sum(by_type.values())

    print(f"Error summary for {start} → {end}: {by_type}, total={total}")

    context["ti"].xcom_push(key="error_by_type", value=by_type)
    context["ti"].xcom_push(key="total_errors", value=total)


def _branch_critical(**context):
    """
    Returns 'send_critical_alert' if the window has critical errors,
    otherwise returns 'log_error_summary'.

    Critical conditions:
      - ANY auth_error events (sign of a compromised or buggy client)
      - Total errors > CRITICAL_TOTAL_ERRORS
    """
    by_type = context["ti"].xcom_pull(task_ids="aggregate_and_classify", key="error_by_type") or {}
    total = context["ti"].xcom_pull(task_ids="aggregate_and_classify", key="total_errors") or 0

    auth_errors = by_type.get("auth_error", 0)
    if auth_errors >= CRITICAL_AUTH_ERRORS or total > CRITICAL_TOTAL_ERRORS:
        return "send_critical_alert"
    return "log_error_summary"


def _send_critical_alert(**context):
    import requests

    by_type = context["ti"].xcom_pull(task_ids="aggregate_and_classify", key="error_by_type") or {}
    total = context["ti"].xcom_pull(task_ids="aggregate_and_classify", key="total_errors") or 0

    message = (
        f":rotating_light: *CRITICAL: Ingestion Error Spike*\n"
        f"*Window:* `{context['data_interval_start']}` → `{context['data_interval_end']}`\n"
        f"*Total errors:* `{total}`\n"
        f"*By type:* `{by_type}`\n"
        f"*Action required:* Check FastAPI logs for upstream client issues"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


def _log_error_summary(**context):
    print(
        f"Error summary logged for window "
        f"{context['data_interval_start']} → {context['data_interval_end']}. "
        f"No critical thresholds breached."
    )


def _update_error_dashboard(**context):
    """
    Upserts aggregated error counts into error_classification_summary.
    This table is what the frontend or Grafana dashboard queries for
    the ingestion error trend chart.
    """
    start = context["data_interval_start"]
    by_type = context["ti"].xcom_pull(task_ids="aggregate_and_classify", key="error_by_type") or {}
    total = sum(by_type.values())

    async def _upsert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            for error_type, count in by_type.items():
                pct = round(count / total * 100, 2) if total > 0 else 0.0
                await conn.execute(
                    """
                    INSERT INTO error_classification_summary
                        (window_start, error_type, error_count, pct_of_total)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (window_start, error_type)
                    DO UPDATE SET
                        error_count  = EXCLUDED.error_count,
                        pct_of_total = EXCLUDED.pct_of_total
                    """,
                    start,
                    error_type,
                    count,
                    pct,
                )
        finally:
            await conn.close()

    asyncio.run(_upsert())


with DAG(
    dag_id="event_ingestion_error_tracker",
    default_args=default_args,
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["errors", "monitoring", "data-quality"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Bootstrap required tables ─────────────────────────────────────
    ensure_error_tables = PostgresOperator(
        task_id="ensure_error_tables",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS ingestion_errors (
                id          BIGSERIAL PRIMARY KEY,
                error_type  VARCHAR(100) NOT NULL DEFAULT 'unknown',
                error_msg   TEXT,
                raw_payload JSONB,
                occurred_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS error_classification_summary (
                window_start TIMESTAMP WITH TIME ZONE NOT NULL,
                error_type   VARCHAR(100) NOT NULL,
                error_count  BIGINT NOT NULL DEFAULT 0,
                pct_of_total NUMERIC(5, 2),
                PRIMARY KEY (window_start, error_type)
            );
        """,
    )

    # ── Step 2: Aggregate errors from this window ─────────────────────────────
    aggregate_and_classify = PythonOperator(
        task_id="aggregate_and_classify",
        python_callable=_aggregate_and_classify,
    )

    # ── Step 3: Decide critical vs normal ─────────────────────────────────────
    branch_critical = BranchPythonOperator(
        task_id="branch_critical",
        python_callable=_branch_critical,
    )

    # ── Branch A: Critical — page the team ────────────────────────────────────
    send_critical_alert = PythonOperator(
        task_id="send_critical_alert",
        python_callable=_send_critical_alert,
    )

    # ── Branch B: Normal — just log it ────────────────────────────────────────
    log_error_summary = PythonOperator(
        task_id="log_error_summary",
        python_callable=_log_error_summary,
    )

    # ── Step 4: Always update dashboard table regardless of branch ────────────
    update_error_dashboard = PythonOperator(
        task_id="update_error_dashboard",
        python_callable=_update_error_dashboard,
        trigger_rule="none_failed_min_one_success",
    )

    ensure_error_tables >> aggregate_and_classify >> branch_critical
    branch_critical >> send_critical_alert >> update_error_dashboard
    branch_critical >> log_error_summary >> update_error_dashboard
