"""
DAG 5: dq_duplicate_detection
Category: Data Quality & Validation

WHAT IT DOES:
  Every 6 hours, finds duplicate events in raw_events — rows where the same
  user_id sent the same event_type within the same second. It uses a SQL
  window function (ROW_NUMBER) to rank duplicates, then logs them to
  dq_duplicate_staging. If duplicates exist, it marks them in an audit log.

WHY WE USE IT:
  Duplicate events are one of the most common bugs in event-driven systems.
  They happen when a client retries a failed HTTP request, Kafka delivers
  a message twice (rare but possible), or a bug causes double-emit.
  Detecting them lets us deduplicate before aggregations, preventing
  inflated purchase counts, revenue figures, and KPIs.

KEY AIRFLOW CONCEPT TAUGHT:
  PostgresOperator with SQL window functions — Airflow's PostgresOperator
  can run any SQL, including complex analytics queries with window functions.
  This shows that Airflow is not just for simple INSERT/SELECT — you can
  express arbitrarily complex transformation logic directly in SQL tasks.
  Also demonstrates Jinja templating {{ data_interval_start }} in SQL strings.
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


def _count_duplicates(**context):
    """
    Reads the dq_duplicate_staging table (populated by the PostgresOperator above)
    and counts how many rows with rn > 1 exist for this window.
    rn > 1 means it's a duplicate (rn=1 is the "original", rn=2,3,... are copies).
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    async def _count():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM dq_duplicate_staging
                WHERE window_start = $1 AND rn > 1
                """,
                start,
            )
        finally:
            await conn.close()
        return count or 0

    count = asyncio.run(_count())
    print(f"Found {count} duplicate rows in window {start} → {end}")
    context["ti"].xcom_push(key="duplicate_count", value=count)


def _branch_has_duplicates(**context):
    count = context["ti"].xcom_pull(task_ids="count_duplicates", key="duplicate_count") or 0
    return "mark_duplicates_in_audit" if count > 0 else "no_duplicates_log"


def _mark_duplicates_in_audit(**context):
    """
    Copies duplicate event IDs from the staging table into a permanent
    audit table (dq_duplicate_audit) for investigation and remediation.
    """
    start = context["data_interval_start"]
    count = context["ti"].xcom_pull(task_ids="count_duplicates", key="duplicate_count") or 0

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dq_duplicate_audit (
                    event_id     BIGINT NOT NULL,
                    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
                    rn           INTEGER NOT NULL,
                    detected_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );

                INSERT INTO dq_duplicate_audit (event_id, window_start, rn)
                SELECT event_id, window_start, rn
                FROM dq_duplicate_staging
                WHERE window_start = $1 AND rn > 1
                ON CONFLICT DO NOTHING
                """,
                start,
            )
        finally:
            await conn.close()

    asyncio.run(_insert())
    print(f"Logged {count} duplicate events to dq_duplicate_audit for window {start}")

    import requests
    message = (
        f":warning: *Duplicate Events Detected*\n"
        f"*Window:* `{start}` → `{context['data_interval_end']}`\n"
        f"*Count:* `{count}` duplicate rows\n"
        f"*Stored in:* `dq_duplicate_audit` table\n"
        f"*Action:* Check upstream client retry logic and Kafka delivery semantics"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


with DAG(
    dag_id="dq_duplicate_detection",
    default_args=default_args,
    schedule_interval="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-quality", "duplicates", "validation"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Create staging table ──────────────────────────────────────────
    create_staging_table = PostgresOperator(
        task_id="create_staging_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS dq_duplicate_staging (
                event_id     BIGINT NOT NULL,
                window_start TIMESTAMP WITH TIME ZONE NOT NULL,
                rn           INTEGER NOT NULL
            );
        """,
    )

    # ── Step 2: Identify duplicates using a window function ───────────────────
    # ROW_NUMBER() OVER (PARTITION BY ...) assigns rank 1 to the first occurrence
    # of each (user_id, event_type, second) group, and rank 2,3,... to duplicates.
    # {{ data_interval_start }} is rendered by Airflow's Jinja engine before the
    # SQL is sent to Postgres — this is safe parameterization via templating.
    identify_duplicates = PostgresOperator(
        task_id="identify_duplicates",
        postgres_conn_id="postgres_default",
        sql="""
            DELETE FROM dq_duplicate_staging
            WHERE window_start = '{{ data_interval_start }}';

            INSERT INTO dq_duplicate_staging (event_id, window_start, rn)
            SELECT
                id                                                    AS event_id,
                '{{ data_interval_start }}'::timestamptz             AS window_start,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, event_type,
                                 DATE_TRUNC('second', created_at)
                    ORDER BY id
                )                                                     AS rn
            FROM raw_events
            WHERE created_at >= '{{ data_interval_start }}'
              AND created_at <  '{{ data_interval_end }}';
        """,
    )

    # ── Step 3: Count how many duplicates exist ────────────────────────────────
    count_duplicates = PythonOperator(
        task_id="count_duplicates",
        python_callable=_count_duplicates,
    )

    # ── Step 4: Branch based on whether duplicates were found ─────────────────
    branch_has_duplicates = BranchPythonOperator(
        task_id="branch_has_duplicates",
        python_callable=_branch_has_duplicates,
    )

    mark_duplicates_in_audit = PythonOperator(
        task_id="mark_duplicates_in_audit",
        python_callable=_mark_duplicates_in_audit,
    )

    no_duplicates_log = EmptyOperator(task_id="no_duplicates_log")

    # ── Step 5: Always clean up staging table ─────────────────────────────────
    # trigger_rule ensures this runs after either branch
    cleanup_staging = PostgresOperator(
        task_id="cleanup_staging",
        postgres_conn_id="postgres_default",
        sql="DELETE FROM dq_duplicate_staging WHERE window_start = '{{ data_interval_start }}';",
        trigger_rule="none_failed_min_one_success",
    )

    create_staging_table >> identify_duplicates >> count_duplicates >> branch_has_duplicates
    branch_has_duplicates >> mark_duplicates_in_audit >> cleanup_staging
    branch_has_duplicates >> no_duplicates_log >> cleanup_staging
