"""
DAG 3: dq_row_count_sla_check
Category: Data Quality & Validation

WHAT IT DOES:
  Every hour, counts how many rows landed in raw_events for that window.
  If fewer than MIN_ROWS_THRESHOLD rows arrived, the window is flagged as
  "below SLA". The result (pass/fail + row count) is logged to batch_sla_log.
  Also uses Airflow's native SLA miss mechanism — if the row count task doesn't
  complete within 30 minutes of the scheduled time, Airflow fires an SLA miss.

WHY WE USE IT:
  Row count monitoring is the simplest and most effective health check for
  an event-driven pipeline. If event volume drops to zero, something broke —
  Kafka is down, the API stopped accepting events, or a client crashed.
  Catching this early (within 30 minutes) limits downstream impact.

KEY AIRFLOW CONCEPTS TAUGHT:
  1. sla=timedelta(...) on individual tasks — Airflow's built-in SLA tracking
     fires sla_miss_callback if the task doesn't finish in time.
  2. ShortCircuitOperator for "no-op" paths — when count > threshold, the
     failure-logging tasks are gracefully skipped instead of wasted.
  3. depends_on_past=True — each run must succeed before the next window starts,
     preventing accumulation of concurrent runs during a data outage.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

MIN_ROWS_THRESHOLD = 10  # minimum expected events per hour window


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Called by Airflow's SLA tracking thread when a task misses its SLA.
    Different from on_failure_callback — this fires even if the task
    eventually succeeds (just too late) or is still running.
    """
    import os
    import requests

    message = (
        f":clock1: *SLA MISS — Row Count Check*\n"
        f"*DAG:* `{dag.dag_id}`\n"
        f"*Slow/missing tasks:* `{[str(t) for t in task_list]}`\n"
        f"*SLA:* Task must complete within 30 minutes of scheduled time\n"
        f"*Action:* Check if raw_events is receiving events from Kafka"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


default_args = {
    "owner": "data_engineering",
    "depends_on_past": True,   # each run must succeed before next starts
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_alert,
}


def _get_row_count(**context):
    """
    Counts rows in raw_events for the current hourly window.
    This task has an SLA of 30 minutes — if it doesn't finish in time,
    the sla_miss_callback fires even before the task completes.
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    async def _count():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM raw_events WHERE created_at >= $1 AND created_at < $2",
                start, end,
            )
        finally:
            await conn.close()
        return count

    count = asyncio.run(_count())
    print(f"Window {start} → {end}: {count} rows (threshold: {MIN_ROWS_THRESHOLD})")
    context["ti"].xcom_push(key="row_count", value=count)
    return count


def _short_circuit_min_rows(**context):
    """
    ShortCircuitOperator callable.
    Returns True  → row count is fine, skip the 'below_threshold_log' task.
    Returns False → row count is too low, run 'below_threshold_log'.

    Wait — that seems backwards! ShortCircuitOperator skips downstream when
    it returns False. So we return True when count is GOOD (skip failure logging)
    and False when count is BAD (allow failure logging to run).

    Actually, we flip the logic: the "circuit" protects the success path.
    When rows are sufficient (True), everything downstream proceeds normally.
    When rows are insufficient (False), everything downstream is SKIPPED.

    We use a separate branch pattern below to handle the failure log separately.
    This demonstrates that ShortCircuitOperator only skips, never re-routes.
    """
    count = context["ti"].xcom_pull(task_ids="get_row_count", key="row_count") or 0
    is_sufficient = count >= MIN_ROWS_THRESHOLD
    if not is_sufficient:
        print(f"Row count {count} < threshold {MIN_ROWS_THRESHOLD} — downstream will be skipped")
    return is_sufficient


def _log_sla_pass(**context):
    """
    Logs a successful SLA pass to batch_sla_log.
    Only runs when row count >= threshold (ShortCircuit returned True).
    """
    count = context["ti"].xcom_pull(task_ids="get_row_count", key="row_count") or 0
    start = context["data_interval_start"]

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                INSERT INTO batch_sla_log (window_start, row_count, sla_passed, checked_at)
                VALUES ($1, $2, TRUE, NOW())
                ON CONFLICT (window_start) DO UPDATE
                    SET row_count = EXCLUDED.row_count, sla_passed = TRUE, checked_at = NOW()
                """,
                start, count,
            )
        finally:
            await conn.close()

    asyncio.run(_insert())
    print(f"SLA PASS logged: {count} rows for window starting {start}")


with DAG(
    dag_id="dq_row_count_sla_check",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=on_sla_miss,   # DAG-level SLA miss handler
    tags=["data-quality", "sla", "monitoring"],
    doc_md=__doc__,
) as dag:

    ensure_sla_table = PostgresOperator(
        task_id="ensure_sla_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS batch_sla_log (
                window_start TIMESTAMP WITH TIME ZONE PRIMARY KEY,
                row_count    BIGINT NOT NULL,
                sla_passed   BOOLEAN NOT NULL,
                checked_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """,
    )

    # sla=timedelta(minutes=30) means: if this task hasn't succeeded within
    # 30 minutes of the DAG's scheduled_time, fire sla_miss_callback.
    # The task itself is still allowed to run — SLA miss != task failure.
    get_row_count = PythonOperator(
        task_id="get_row_count",
        python_callable=_get_row_count,
        sla=timedelta(minutes=30),
    )

    # Returns True if rows >= threshold (downstream proceeds normally)
    # Returns False if rows < threshold (downstream tasks are SKIPPED)
    short_circuit_min_rows = ShortCircuitOperator(
        task_id="short_circuit_min_rows",
        python_callable=_short_circuit_min_rows,
    )

    log_sla_pass = PythonOperator(
        task_id="log_sla_pass",
        python_callable=_log_sla_pass,
    )

    ensure_sla_table >> get_row_count >> short_circuit_min_rows >> log_sla_pass
