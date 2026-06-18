"""
DAG 4: dq_event_type_distribution
Category: Data Quality & Validation

WHAT IT DOES:
  Every day at 08:00, computes the percentage share of each event_type
  (purchase, view, add_to_cart, checkout, refund) for yesterday vs the
  7-day rolling average. If any event_type's share shifts by more than
  DRIFT_THRESHOLD_PCT percentage points, it flags a distribution drift
  and stores the finding in dq_distribution_audit.

WHY WE USE IT:
  A sudden drop in "purchase" events from 20% to 2% is almost certainly
  a bug — not a real business change. Distribution monitoring catches these
  silent data quality failures that row-count checks miss entirely.

KEY AIRFLOW CONCEPT TAUGHT:
  XCom chaining — multiple tasks pass intermediate results to each other
  through XCom. Task A pushes → Task B pulls → Task C pulls from both A and B.
  This is the standard pattern for multi-step Python analysis in Airflow.
  Also teaches storing audit records to Postgres for historical trend analysis.
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

DRIFT_THRESHOLD_PCT = 10.0  # flag if any event_type shifts by more than 10 percentage points


async def _fetch_distribution(conn, start_iso: str, end_iso: str) -> dict:
    """
    Fetches the percentage distribution of event_types between two timestamps.
    Returns a dict like: {"purchase": 22.5, "view": 45.1, "checkout": 12.0, ...}
    """
    rows = await conn.fetch(
        """
        SELECT
            event_type,
            COUNT(*) AS cnt,
            ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct
        FROM raw_events
        WHERE created_at >= $1::timestamptz
          AND created_at <  $2::timestamptz
        GROUP BY event_type
        ORDER BY pct DESC
        """,
        start_iso, end_iso,
    )
    return {r["event_type"]: float(r["pct"]) for r in rows}


def _compute_today_distribution(**context):
    """
    Computes event_type distribution for yesterday (the reporting day).
    {{ yesterday_ds }} is yesterday's date as a string "YYYY-MM-DD".
    """
    yesterday = context["yesterday_ds"]
    today = context["ds"]

    async def _run():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            return await _fetch_distribution(
                conn,
                f"{yesterday} 00:00:00+00",
                f"{today} 00:00:00+00",
            )
        finally:
            await conn.close()

    dist = asyncio.run(_run())
    print(f"Yesterday's distribution: {dist}")
    context["ti"].xcom_push(key="today_dist", value=dist)


def _compute_baseline_distribution(**context):
    """
    Computes the 7-day rolling average distribution as a baseline.
    Using 7 days captures weekly seasonality (weekends differ from weekdays).
    """
    yesterday = context["yesterday_ds"]
    seven_days_ago = (datetime.strptime(yesterday, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

    async def _run():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            return await _fetch_distribution(
                conn,
                f"{seven_days_ago} 00:00:00+00",
                f"{yesterday} 00:00:00+00",
            )
        finally:
            await conn.close()

    baseline = asyncio.run(_run())
    print(f"7-day baseline distribution: {baseline}")
    context["ti"].xcom_push(key="baseline_dist", value=baseline)


def _compare_distributions(**context):
    """
    Computes the absolute percentage point difference for each event_type
    between today and the 7-day baseline. Flags drift if any exceeds threshold.
    """
    today_dist = context["ti"].xcom_pull(task_ids="compute_today_distribution", key="today_dist") or {}
    baseline_dist = context["ti"].xcom_pull(task_ids="compute_baseline_distribution", key="baseline_dist") or {}

    all_types = set(today_dist.keys()) | set(baseline_dist.keys())
    deviations = {}
    for et in all_types:
        today_pct = today_dist.get(et, 0.0)
        baseline_pct = baseline_dist.get(et, 0.0)
        deviations[et] = abs(today_pct - baseline_pct)

    max_deviation = max(deviations.values()) if deviations else 0.0
    drift_detected = max_deviation >= DRIFT_THRESHOLD_PCT

    print(f"Max deviation: {max_deviation:.2f}% (threshold: {DRIFT_THRESHOLD_PCT}%)")
    print(f"Deviations by type: {deviations}")

    context["ti"].xcom_push(key="drift_detected", value=drift_detected)
    context["ti"].xcom_push(key="max_deviation_pct", value=max_deviation)
    context["ti"].xcom_push(key="deviations", value=deviations)


def _branch_result(**context):
    drift = context["ti"].xcom_pull(task_ids="compare_distributions", key="drift_detected")
    return "flag_distribution_drift" if drift else "distributions_healthy"


def _flag_distribution_drift(**context):
    import requests

    deviations = context["ti"].xcom_pull(task_ids="compare_distributions", key="deviations") or {}
    max_dev = context["ti"].xcom_pull(task_ids="compare_distributions", key="max_deviation_pct") or 0

    message = (
        f":bar_chart: *Distribution Drift Detected*\n"
        f"*Date:* `{context['yesterday_ds']}`\n"
        f"*Max deviation:* `{max_dev:.2f}%` (threshold: {DRIFT_THRESHOLD_PCT}%)\n"
        f"*By event type:* `{deviations}`\n"
        f"Check for: missing event types, upstream client changes, Kafka consumer issues"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


def _store_audit_record(**context):
    """
    Always writes the drift check result to dq_distribution_audit,
    whether drift was detected or not. This builds a history of daily
    distribution health that can be queried for trend analysis.
    """
    drift = context["ti"].xcom_pull(task_ids="compare_distributions", key="drift_detected") or False
    max_dev = context["ti"].xcom_pull(task_ids="compare_distributions", key="max_deviation_pct") or 0.0
    today_dist = context["ti"].xcom_pull(task_ids="compute_today_distribution", key="today_dist") or {}
    baseline_dist = context["ti"].xcom_pull(task_ids="compute_baseline_distribution", key="baseline_dist") or {}

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                INSERT INTO dq_distribution_audit
                    (report_date, drift_detected, max_deviation_pct, today_dist, baseline_dist)
                VALUES ($1::date, $2, $3, $4::jsonb, $5::jsonb)
                ON CONFLICT (report_date) DO UPDATE
                    SET drift_detected    = EXCLUDED.drift_detected,
                        max_deviation_pct = EXCLUDED.max_deviation_pct,
                        today_dist        = EXCLUDED.today_dist,
                        baseline_dist     = EXCLUDED.baseline_dist
                """,
                context["yesterday_ds"],
                drift,
                max_dev,
                str(today_dist),
                str(baseline_dist),
            )
        finally:
            await conn.close()

    asyncio.run(_insert())


with DAG(
    dag_id="dq_event_type_distribution",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-quality", "distribution", "monitoring"],
    doc_md=__doc__,
) as dag:

    ensure_audit_table = PostgresOperator(
        task_id="ensure_audit_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS dq_distribution_audit (
                report_date       DATE PRIMARY KEY,
                drift_detected    BOOLEAN NOT NULL,
                max_deviation_pct NUMERIC(5, 2),
                today_dist        JSONB,
                baseline_dist     JSONB,
                checked_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """,
    )

    # These two tasks run in PARALLEL — they're independent queries
    compute_today = PythonOperator(
        task_id="compute_today_distribution",
        python_callable=_compute_today_distribution,
    )

    compute_baseline = PythonOperator(
        task_id="compute_baseline_distribution",
        python_callable=_compute_baseline_distribution,
    )

    compare_distributions = PythonOperator(
        task_id="compare_distributions",
        python_callable=_compare_distributions,
    )

    branch_result = BranchPythonOperator(
        task_id="branch_result",
        python_callable=_branch_result,
    )

    flag_distribution_drift = PythonOperator(
        task_id="flag_distribution_drift",
        python_callable=_flag_distribution_drift,
    )

    distributions_healthy = EmptyOperator(task_id="distributions_healthy")

    store_audit_record = PythonOperator(
        task_id="store_audit_record",
        python_callable=_store_audit_record,
        trigger_rule="none_failed_min_one_success",
    )

    ensure_audit_table >> [compute_today, compute_baseline] >> compare_distributions
    compare_distributions >> branch_result
    branch_result >> flag_distribution_drift >> store_audit_record
    branch_result >> distributions_healthy >> store_audit_record
