"""
DAG 12: monthly_revenue_kpi_report
Category: Reporting & Aggregation

WHAT IT DOES:
  On the 1st of each month at 08:00, computes KPIs for the previous month:
    - Total purchase events
    - Unique buyers (distinct user_ids who purchased)
    - Average "amount" from event payload JSONB field
  Stores results in monthly_kpi_report table, validates sanity (no zeros),
  then triggers archive_and_purge_users DAG as a downstream dependent.

WHY WE USE IT:
  Monthly KPI reporting is a standard business requirement. Using JSONB operators
  (->>, ::NUMERIC) to extract numeric values from unstructured payload data is
  a key Postgres skill for analytics engineers. The TriggerDagRunOperator shows
  how to chain DAG executions — after reporting, kick off maintenance.

KEY AIRFLOW CONCEPT TAUGHT:
  TriggerDagRunOperator — triggers another DAG from within a running DAG.
  Key parameters:
    wait_for_completion=False: fire and forget (monthly report doesn't wait
    for the archive to finish)
    conf={...}: passes runtime configuration to the triggered DAG
  Also teaches JSONB extraction in Postgres for semi-structured payload data.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}


def _compute_monthly_kpis(**context):
    """
    Computes purchase KPIs for the previous calendar month.

    The payload->>'amount' syntax is PostgreSQL JSONB notation:
      payload          → the JSONB column
      ->>'amount'      → extracts 'amount' field as TEXT
      ::NUMERIC        → casts text to number for AVG calculation

    This works because our raw_events.payload stores event details as JSON,
    e.g.: {"product_id": 42, "amount": 99.99, "currency": "USD"}
    """
    # First day of last month
    run_date = datetime.strptime(context["ds"], "%Y-%m-%d")
    first_of_this_month = run_date.replace(day=1)
    first_of_last_month = (first_of_this_month - timedelta(days=1)).replace(day=1)
    report_month = first_of_last_month.strftime("%Y-%m")

    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE event_type = 'purchase')         AS total_purchases,
                    COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'purchase') AS unique_buyers,
                    AVG((payload->>'amount')::NUMERIC)
                        FILTER (WHERE event_type = 'purchase'
                                  AND payload->>'amount' IS NOT NULL
                                  AND (payload->>'amount')::TEXT ~ '^[0-9.]+$')
                                                                             AS avg_amount
                FROM raw_events
                WHERE created_at >= $1
                  AND created_at <  $2
                """,
                first_of_last_month,
                first_of_this_month,
            )
        finally:
            await conn.close()
        return row

    result = asyncio.run(_query())
    kpis = {
        "report_month": report_month,
        "total_purchases": result["total_purchases"] or 0,
        "unique_buyers": result["unique_buyers"] or 0,
        "avg_amount": float(result["avg_amount"] or 0.0),
    }
    print(f"Monthly KPIs for {report_month}: {kpis}")
    context["ti"].xcom_push(key="kpis", value=kpis)
    context["ti"].xcom_push(key="report_month", value=report_month)


def _validate_kpi_sanity(**context):
    """
    Sanity check: if total_purchases is 0, that's suspicious for a live platform.
    Route to warning branch rather than failing — 0 purchases might be correct
    during initial deployment or low-traffic periods.
    """
    kpis = context["ti"].xcom_pull(task_ids="compute_monthly_kpis", key="kpis") or {}
    total = kpis.get("total_purchases", 0)
    if total == 0:
        return "kpi_zero_warning"
    return "store_kpi_table"


def _kpi_zero_warning(**context):
    import requests

    month = context["ti"].xcom_pull(task_ids="compute_monthly_kpis", key="report_month") or "?"
    message = (
        f":question: *Monthly KPI Warning — Zero Purchases*\n"
        f"*Month:* `{month}`\n"
        f"*Total purchases:* `0`\n"
        f"This may be expected during initial deployment. "
        f"Verify that purchase events are being ingested correctly."
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


def _store_kpi_table(**context):
    kpis = context["ti"].xcom_pull(task_ids="compute_monthly_kpis", key="kpis") or {}

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monthly_kpi_report (
                    report_month    VARCHAR(7) PRIMARY KEY,  -- "YYYY-MM"
                    total_purchases BIGINT NOT NULL DEFAULT 0,
                    unique_buyers   BIGINT NOT NULL DEFAULT 0,
                    avg_amount      NUMERIC(12, 2),
                    generated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );

                INSERT INTO monthly_kpi_report
                    (report_month, total_purchases, unique_buyers, avg_amount)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (report_month) DO UPDATE SET
                    total_purchases = EXCLUDED.total_purchases,
                    unique_buyers   = EXCLUDED.unique_buyers,
                    avg_amount      = EXCLUDED.avg_amount,
                    generated_at    = NOW()
                """,
                kpis["report_month"],
                kpis["total_purchases"],
                kpis["unique_buyers"],
                kpis["avg_amount"],
            )
        finally:
            await conn.close()

    asyncio.run(_insert())
    print(f"Stored KPIs: {kpis}")


with DAG(
    dag_id="monthly_revenue_kpi_report",
    default_args=default_args,
    schedule_interval="0 8 1 * *",   # 1st of each month, 08:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reporting", "kpi", "monthly"],
    doc_md=__doc__,
) as dag:

    compute_monthly_kpis = PythonOperator(
        task_id="compute_monthly_kpis",
        python_callable=_compute_monthly_kpis,
    )

    validate_kpi_sanity = BranchPythonOperator(
        task_id="validate_kpi_sanity",
        python_callable=_validate_kpi_sanity,
    )

    kpi_zero_warning = PythonOperator(
        task_id="kpi_zero_warning",
        python_callable=_kpi_zero_warning,
    )

    store_kpi_table = PythonOperator(
        task_id="store_kpi_table",
        python_callable=_store_kpi_table,
    )

    # ── TriggerDagRunOperator — kicks off archive DAG after monthly report ────
    # wait_for_completion=False means this task completes immediately after
    # triggering, without waiting for archive_and_purge_users to finish.
    # conf= passes data to the triggered DAG (accessible via context['dag_run'].conf)
    trigger_archive_dag = TriggerDagRunOperator(
        task_id="trigger_archive_dag",
        trigger_dag_id="archive_and_purge_users",
        wait_for_completion=False,
        conf={"triggered_by": "monthly_revenue_kpi_report", "month": "{{ ds }}"},
        trigger_rule="none_failed_min_one_success",
    )

    generate_kpi_summary_log = PythonOperator(
        task_id="generate_kpi_summary_log",
        python_callable=lambda **ctx: print(
            f"Monthly KPI report complete for "
            f"{ctx['ti'].xcom_pull(task_ids='compute_monthly_kpis', key='report_month')}. "
            f"Archive DAG triggered."
        ),
        trigger_rule="none_failed_min_one_success",
    )

    compute_monthly_kpis >> validate_kpi_sanity
    validate_kpi_sanity >> kpi_zero_warning >> trigger_archive_dag
    validate_kpi_sanity >> store_kpi_table >> trigger_archive_dag
    trigger_archive_dag >> generate_kpi_summary_log
