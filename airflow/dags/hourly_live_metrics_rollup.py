"""
DAG 14: hourly_live_metrics_rollup
Category: Reporting & Aggregation

WHAT IT DOES:
  Every hour, rolls up the per-minute data from live_event_metrics (written
  by Spark Streaming every 10 seconds) into hourly aggregates in
  hourly_metrics_rollup. Produces: total events per type per hour, average
  events per minute. If the streaming job wrote no data for this hour (e.g.,
  it was restarting), the DAG silently skips instead of writing empty rows.

WHY WE USE IT:
  Spark Streaming writes to live_event_metrics every ~10 seconds. That's
  360 rows per hour per event type — too granular for hourly trend charts.
  This rollup compresses 360 rows into 1 row per event type per hour,
  making hourly and daily dashboards fast to query.

KEY AIRFLOW CONCEPT TAUGHT:
  ShortCircuitOperator as a "data guard" — it protects downstream tasks from
  running when there's no data to process. This is the right pattern when an
  empty-window is a normal, expected state (streaming job briefly down).
  Also shows how Airflow batch DAGs complement real-time streaming systems —
  they're not competing, they're collaborating at different time granularities.
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

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}


def _check_streaming_data_exists(**context):
    """
    Returns True if live_event_metrics has rows for this hour's window.
    Returns False to skip all downstream tasks if streaming job wrote nothing.

    This handles:
    - Normal operation: streaming job is running → True, rollup runs
    - Streaming job restarting: no data yet → False, rollup skipped cleanly
    - First deployment: table is empty → False, no errors
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    async def _count():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM live_event_metrics
                WHERE window_start >= $1 AND window_start < $2
                """,
                start, end,
            )
        finally:
            await conn.close()
        return count or 0

    count = asyncio.run(_count())
    print(f"Streaming metrics rows for window {start} → {end}: {count}")
    context["ti"].xcom_push(key="row_count", value=count)
    return count > 0


def _update_rollup_metadata(**context):
    """
    After the SQL rollup task completes, logs the metadata for this rollup run.
    """
    row_count = context["ti"].xcom_pull(task_ids="check_streaming_data_exists", key="row_count") or 0
    print(f"Hourly rollup complete for {context['data_interval_start']}: "
          f"rolled up {row_count} streaming rows into hourly_metrics_rollup")


with DAG(
    dag_id="hourly_live_metrics_rollup",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reporting", "aggregation", "streaming"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Bootstrap the rollup target table ─────────────────────────────
    ensure_rollup_table = PostgresOperator(
        task_id="ensure_rollup_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS hourly_metrics_rollup (
                hour_start      TIMESTAMP WITH TIME ZONE NOT NULL,
                event_type      VARCHAR(50) NOT NULL,
                total_count     BIGINT NOT NULL,
                avg_per_minute  NUMERIC(10, 2),
                updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (hour_start, event_type)
            );
        """,
    )

    # ── Step 2: Skip if streaming job wrote nothing this hour ─────────────────
    check_streaming_data_exists = ShortCircuitOperator(
        task_id="check_streaming_data_exists",
        python_callable=_check_streaming_data_exists,
    )

    # ── Step 3: Rollup per-minute data into hourly aggregates ─────────────────
    # DATE_TRUNC('hour', window_start) collapses all per-minute windows into
    # a single hour bucket. SUM(event_count) totals all events in that hour.
    # avg_per_minute = total / 60 gives the average events per minute.
    # ON CONFLICT DO UPDATE makes this idempotent — re-running the same hour
    # overwrites the previous result with updated numbers.
    rollup_to_hourly = PostgresOperator(
        task_id="rollup_to_hourly",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO hourly_metrics_rollup
                (hour_start, event_type, total_count, avg_per_minute, updated_at)
            SELECT
                DATE_TRUNC('hour', window_start)          AS hour_start,
                event_type,
                SUM(event_count)                          AS total_count,
                ROUND(SUM(event_count)::NUMERIC / 60, 2)  AS avg_per_minute,
                NOW()                                     AS updated_at
            FROM live_event_metrics
            WHERE window_start >= '{{ data_interval_start }}'
              AND window_start <  '{{ data_interval_end }}'
            GROUP BY DATE_TRUNC('hour', window_start), event_type
            ON CONFLICT (hour_start, event_type)
            DO UPDATE SET
                total_count    = EXCLUDED.total_count,
                avg_per_minute = EXCLUDED.avg_per_minute,
                updated_at     = NOW();
        """,
    )

    # ── Step 4: Log metadata ──────────────────────────────────────────────────
    update_rollup_metadata = PythonOperator(
        task_id="update_rollup_metadata",
        python_callable=_update_rollup_metadata,
    )

    ensure_rollup_table >> check_streaming_data_exists >> rollup_to_hourly >> update_rollup_metadata
