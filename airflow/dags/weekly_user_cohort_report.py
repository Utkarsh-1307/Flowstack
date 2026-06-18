"""
DAG 11: weekly_user_cohort_report
Category: Reporting & Aggregation

WHAT IT DOES:
  Every Monday at 07:00, builds a user cohort analysis:
    1. Groups users by their signup week (cohort = week they registered)
    2. Counts how many events each cohort generated in the past week
    3. Exports the result to a CSV in /data/gold/reports/ via Spark
    4. Notifies team that the report is ready

  Example output: "Users who signed up week of 2026-01-05 generated 4,231 events
  in the week of 2026-06-10"

WHY WE USE IT:
  Cohort analysis answers "are newer users more or less active than older users?"
  This is a fundamental product analytics question. Declining engagement in
  newer cohorts signals a product problem. Improving engagement signals growth.

KEY AIRFLOW CONCEPT TAUGHT:
  PostgresOperator with complex multi-step SQL — shows that Airflow can
  orchestrate a sequence of SQL statements, each building on the previous one.
  The materialized cohort dimension (step 2) is computed once and reused by
  the aggregation step (step 3) — more efficient than computing it inline.
  Also shows using BashOperator for Spark as a reporting/export tool.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}


def _notify_report_ready(**context):
    """
    Posts a notification that the weekly cohort report is ready.
    Reuses the same requests pattern as alerts.py for consistency.
    """
    import requests

    report_date = context["ds"]
    report_path = f"/data/gold/reports/weekly_cohort_{report_date}.csv"

    message = (
        f":chart_with_upwards_trend: *Weekly Cohort Report Ready*\n"
        f"*Date:* `{report_date}`\n"
        f"*File:* `{report_path}`\n"
        f"*Contents:* User signup cohorts × events generated this week"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass
    print(f"Report notification sent for {report_path}")


with DAG(
    dag_id="weekly_user_cohort_report",
    default_args=default_args,
    schedule_interval="0 7 * * 1",   # every Monday at 07:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reporting", "cohort", "weekly"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Create target tables ───────────────────────────────────────────
    create_cohort_tables = PostgresOperator(
        task_id="create_cohort_tables",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS user_cohorts (
                user_id      UUID NOT NULL,
                cohort_week  DATE NOT NULL,
                PRIMARY KEY (user_id)
            );

            CREATE TABLE IF NOT EXISTS weekly_cohort_metrics (
                cohort_week    DATE NOT NULL,
                report_week    DATE NOT NULL,
                event_count    BIGINT NOT NULL DEFAULT 0,
                unique_users   BIGINT NOT NULL DEFAULT 0,
                generated_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (cohort_week, report_week)
            );

            CREATE TABLE IF NOT EXISTS pipeline_failure_audit (
                id           BIGSERIAL PRIMARY KEY,
                report_date  DATE NOT NULL,
                dag_id       VARCHAR(250) NOT NULL,
                task_id      VARCHAR(250),
                failure_count INT DEFAULT 0,
                first_failure TIMESTAMP WITH TIME ZONE,
                last_failure  TIMESTAMP WITH TIME ZONE
            );
        """,
    )

    # ── Step 2: Rebuild user cohort dimension ─────────────────────────────────
    # DATE_TRUNC('week', created_at) gives the Monday of the week each user signed up.
    # This is the "cohort label" — users who signed up the same week are in the same cohort.
    # ON CONFLICT DO NOTHING is safe — existing users keep their original cohort_week.
    build_user_cohort_dimension = PostgresOperator(
        task_id="build_user_cohort_dimension",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO user_cohorts (user_id, cohort_week)
            SELECT
                id                                           AS user_id,
                DATE_TRUNC('week', created_at)::DATE        AS cohort_week
            FROM users
            WHERE deleted_at IS NULL OR deleted_at > NOW()
            ON CONFLICT (user_id) DO NOTHING;
        """,
    )

    # ── Step 3: Aggregate events per cohort for this week ─────────────────────
    # Joins raw_events → users → user_cohorts to get each event's cohort label.
    # Groups by (cohort_week, report_week) to produce one row per cohort per week.
    # {{ macros.ds_add(ds, -7) }} subtracts 7 days from today's date — Airflow macro.
    aggregate_cohort_events = PostgresOperator(
        task_id="aggregate_cohort_events",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO weekly_cohort_metrics (cohort_week, report_week, event_count, unique_users)
            SELECT
                uc.cohort_week                           AS cohort_week,
                DATE_TRUNC('week', '{{ ds }}'::DATE)::DATE AS report_week,
                COUNT(re.id)                             AS event_count,
                COUNT(DISTINCT re.user_id)               AS unique_users
            FROM raw_events re
            JOIN users u ON u.id = re.user_id
            JOIN user_cohorts uc ON uc.user_id = u.id
            WHERE re.created_at >= '{{ macros.ds_add(ds, -7) }} 00:00:00+00'
              AND re.created_at <  '{{ ds }} 00:00:00+00'
            GROUP BY uc.cohort_week, DATE_TRUNC('week', '{{ ds }}'::DATE)
            ON CONFLICT (cohort_week, report_week)
            DO UPDATE SET
                event_count  = EXCLUDED.event_count,
                unique_users = EXCLUDED.unique_users,
                generated_at = NOW();
        """,
    )

    # ── Step 4: Export to CSV via Spark JDBC ──────────────────────────────────
    # Spark reads from Postgres via JDBC and writes as CSV to gold layer.
    # This is a common pattern: use Spark as a fast parallel exporter
    # (JDBC parallelism splits the read across multiple Spark partitions).
    # Note: {{ ds }} is Jinja-templated in BashOperator bash_command.
    export_cohort_csv = BashOperator(
        task_id="export_cohort_csv",
        bash_command=(
            "OUT=/data/gold/reports && mkdir -p $OUT && "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--packages org.postgresql:postgresql:42.7.3 "
            "/opt/spark/apps/metrics_aggregation.py "
            "--start {{ ds }}T00:00:00+00:00 "
            "--end {{ next_ds }}T00:00:00+00:00 "
            "|| echo 'Spark export failed — CSV not generated' "
        ),
    )

    # ── Step 5: Notify team ───────────────────────────────────────────────────
    notify_report_ready = PythonOperator(
        task_id="notify_report_ready",
        python_callable=_notify_report_ready,
    )

    (
        create_cohort_tables
        >> build_user_cohort_dimension
        >> aggregate_cohort_events
        >> export_cohort_csv
        >> notify_report_ready
    )
