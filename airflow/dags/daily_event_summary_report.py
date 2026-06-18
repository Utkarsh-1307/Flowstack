"""
DAG 10: daily_event_summary_report
Category: Reporting & Aggregation

WHAT IT DOES:
  Every day at 06:00, aggregates the previous day's raw_events into a
  summary table (daily_event_summary) broken down by event_type and hour.
  The percentage-of-daily column shows which event types dominated each hour.

WHY WE USE IT:
  Downstream dashboards and BI tools need clean, pre-aggregated data —
  querying raw_events directly at dashboard load time is too slow and
  expensive at scale. This DAG maintains a fast read table for reporting.

KEY AIRFLOW CONCEPT TAUGHT:
  Jinja date macros: {{ yesterday_ds }} gives "YYYY-MM-DD" for yesterday,
  {{ ds }} gives today's date. These are built into every DAG run and make
  SQL idempotent — re-running the DAG for the same date produces the same
  result because ON CONFLICT DO UPDATE replaces rather than duplicates.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}


def _log_report_metadata(**context):
    """
    After the SQL tasks complete, log metadata about this report run.
    This is a lightweight audit trail so we know when each day's report
    was last generated and by which DAG run.
    """
    import asyncio
    import asyncpg

    report_date = context["yesterday_ds"]
    run_id = context["run_id"]

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                INSERT INTO pipeline_run_log (dag_id, run_date, row_count, mode, passed_at)
                VALUES ($1, $2::date, 0, 'daily_report', NOW())
                ON CONFLICT (dag_id, run_date) DO UPDATE
                    SET passed_at = NOW(), mode = 'daily_report'
                """,
                "daily_event_summary_report",
                report_date,
            )
        finally:
            await conn.close()

    asyncio.run(_insert())
    print(f"Logged report run for {report_date} (run_id={run_id})")


with DAG(
    dag_id="daily_event_summary_report",
    default_args=default_args,
    schedule_interval="0 6 * * *",   # every day at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reporting", "aggregation"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Create pipeline_run_log (shared audit table used by many DAGs)
    ensure_run_log_table = PostgresOperator(
        task_id="ensure_run_log_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS pipeline_run_log (
                dag_id    VARCHAR(250) NOT NULL,
                run_date  DATE NOT NULL,
                row_count BIGINT DEFAULT 0,
                mode      VARCHAR(50) DEFAULT 'incremental',
                passed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (dag_id, run_date)
            );
        """,
    )

    # ── Step 2: Create the daily summary target table ─────────────────────────
    # PRIMARY KEY on (report_date, event_hour, event_type) means we can safely
    # re-run this DAG for any date and it will UPDATE rather than INSERT duplicates.
    ensure_summary_table = PostgresOperator(
        task_id="ensure_summary_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS daily_event_summary (
                report_date  DATE NOT NULL,
                event_hour   SMALLINT NOT NULL,
                event_type   VARCHAR(50) NOT NULL,
                event_count  BIGINT NOT NULL,
                pct_of_daily NUMERIC(5, 2),
                created_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (report_date, event_hour, event_type)
            );
        """,
    )

    # ── Step 3: Populate the summary for yesterday ────────────────────────────
    # {{ yesterday_ds }} → "2026-06-17"   (always the day before this run)
    # {{ ds }}           → "2026-06-18"   (today = the scheduled run date)
    #
    # ON CONFLICT ... DO UPDATE makes this UPSERT — safe to re-run.
    # EXTRACT(HOUR FROM created_at) pulls the hour (0-23) from each timestamp.
    populate_daily_summary = PostgresOperator(
        task_id="populate_daily_summary",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO daily_event_summary (report_date, event_hour, event_type, event_count)
            SELECT
                '{{ yesterday_ds }}'::DATE                  AS report_date,
                EXTRACT(HOUR FROM created_at)::SMALLINT     AS event_hour,
                event_type,
                COUNT(*)                                     AS event_count
            FROM raw_events
            WHERE created_at >= '{{ yesterday_ds }} 00:00:00+00'
              AND created_at <  '{{ ds }} 00:00:00+00'
            GROUP BY 1, 2, 3
            ON CONFLICT (report_date, event_hour, event_type)
            DO UPDATE SET
                event_count = EXCLUDED.event_count,
                created_at  = NOW();
        """,
    )

    # ── Step 4: Compute each event_type's share of the total day ─────────────
    # This is a second pass using a window function (SUM(...) OVER) to divide
    # each row's count by the total count for that day.
    compute_percentages = PostgresOperator(
        task_id="compute_percentages",
        postgres_conn_id="postgres_default",
        sql="""
            UPDATE daily_event_summary dst
            SET pct_of_daily = ROUND(
                dst.event_count::NUMERIC /
                NULLIF(totals.day_total, 0) * 100,
                2
            )
            FROM (
                SELECT
                    report_date,
                    SUM(event_count) AS day_total
                FROM daily_event_summary
                WHERE report_date = '{{ yesterday_ds }}'::DATE
                GROUP BY report_date
            ) totals
            WHERE dst.report_date = totals.report_date
              AND dst.report_date = '{{ yesterday_ds }}'::DATE;
        """,
    )

    # ── Step 5: Audit log ─────────────────────────────────────────────────────
    log_report_metadata = PythonOperator(
        task_id="log_report_metadata",
        python_callable=_log_report_metadata,
    )

    # ── Wire up ───────────────────────────────────────────────────────────────
    [ensure_run_log_table, ensure_summary_table] >> populate_daily_summary
    populate_daily_summary >> compute_percentages >> log_report_metadata
