"""
DAG 7: cleanup_old_live_metrics
Category: Data Lifecycle & Cleanup

WHAT IT DOES:
  Every Sunday at 03:00 AM, deletes rows from live_event_metrics that are
  older than 90 days. Does it in batches of 10,000 rows to avoid long-running
  transactions that could lock the table and block the Spark Streaming writer.
  Finally runs VACUUM ANALYZE to reclaim disk space and update query planner stats.

WHY WE USE IT:
  live_event_metrics is written by Spark Streaming every 10 seconds.
  That's ~8,640 rows/day, ~259,200 rows/month. Without cleanup, this table
  grows indefinitely. Old streaming metrics are rarely queried — keeping only
  90 days gives plenty of trend data while keeping the table lean.

KEY AIRFLOW CONCEPT TAUGHT:
  Batched deletes in PythonOperator — deleting millions of rows in one SQL
  statement takes a long lock that can block other writers. The while-loop
  pattern (delete 10K at a time, sleep briefly, repeat) is the production-safe
  way to do large table cleanups. Also demonstrates why VACUUM must use
  autocommit mode (it cannot run inside a transaction block).
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}

RETENTION_DAYS = 90
BATCH_SIZE = 10_000


def _check_table_size(**context):
    """
    Checks how large live_event_metrics is before cleanup.
    pg_total_relation_size includes indexes. pg_size_pretty formats bytes
    as "42 MB" — human-readable without needing external tools.
    """
    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            row = await conn.fetchrow(
                """
                SELECT
                    pg_size_pretty(pg_total_relation_size('live_event_metrics')) AS size,
                    COUNT(*) AS total_rows
                FROM live_event_metrics
                """
            )
        finally:
            await conn.close()
        return row

    result = asyncio.run(_query())
    size = result["size"]
    rows = result["total_rows"]
    print(f"live_event_metrics: {rows:,} rows, {size} total (with indexes)")
    context["ti"].xcom_push(key="before_size", value=size)
    context["ti"].xcom_push(key="before_rows", value=rows)


def _delete_old_metrics_in_batches(**context):
    """
    Deletes rows older than RETENTION_DAYS, BATCH_SIZE rows at a time.

    WHY BATCHES?
    A single DELETE ... WHERE recorded_at < NOW() - INTERVAL '90 days'
    on a large table acquires a long table lock. During that lock, the
    Spark Streaming job (which writes every 10 seconds) is blocked.
    Batching keeps individual transactions small and fast.

    The DELETE ... WHERE id IN (SELECT id ... LIMIT 10000) pattern
    is the safest way to batch large deletes — it selects a manageable
    set of IDs first, then deletes only those rows.
    """
    async def _delete():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        total_deleted = 0
        try:
            while True:
                # Delete a batch of old rows
                deleted_ids = await conn.fetch(
                    f"""
                    DELETE FROM live_event_metrics
                    WHERE id IN (
                        SELECT id FROM live_event_metrics
                        WHERE recorded_at < NOW() - INTERVAL '{RETENTION_DAYS} days'
                        LIMIT {BATCH_SIZE}
                    )
                    RETURNING id
                    """
                )
                batch_count = len(deleted_ids)
                total_deleted += batch_count
                print(f"Deleted batch of {batch_count} rows (total so far: {total_deleted:,})")

                if batch_count == 0:
                    break  # no more old rows to delete

                # Brief pause to let Spark Streaming writes through
                await asyncio.sleep(0.1)

        finally:
            await conn.close()
        return total_deleted

    total = asyncio.run(_delete())
    print(f"Total rows deleted from live_event_metrics: {total:,}")
    context["ti"].xcom_push(key="rows_deleted", value=total)


def _vacuum_analyze_table(**context):
    """
    Runs VACUUM ANALYZE on live_event_metrics.

    WHY NOT PostgresOperator?
    VACUUM cannot run inside a transaction block (it's a maintenance operation).
    PostgresOperator wraps every SQL in a transaction by default, so it would fail.
    We use a raw asyncpg connection with manual autocommit-equivalent behavior.

    VACUUM reclaims disk space from deleted rows (PostgreSQL marks deleted rows
    as "dead" but doesn't immediately reclaim disk space — VACUUM does the cleanup).
    ANALYZE updates the query planner's statistics so SELECT queries stay fast.
    """
    async def _vacuum():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        # asyncpg doesn't have autocommit mode — we issue VACUUM in isolation
        # by ensuring no active transaction. VACUUM cannot be inside BEGIN...COMMIT.
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute("VACUUM ANALYZE live_event_metrics")
            print("VACUUM ANALYZE completed on live_event_metrics")
        finally:
            await conn.close()

    asyncio.run(_vacuum())


def _log_maintenance_record(**context):
    before_rows = context["ti"].xcom_pull(task_ids="check_table_size", key="before_rows") or 0
    before_size = context["ti"].xcom_pull(task_ids="check_table_size", key="before_size") or "?"
    rows_deleted = context["ti"].xcom_pull(task_ids="delete_old_metrics_in_batches", key="rows_deleted") or 0

    print(
        f"Maintenance complete:\n"
        f"  Before: {before_rows:,} rows ({before_size})\n"
        f"  Deleted: {rows_deleted:,} rows\n"
        f"  After: ~{before_rows - rows_deleted:,} rows\n"
        f"  Retention: {RETENTION_DAYS} days"
    )


with DAG(
    dag_id="cleanup_old_live_metrics",
    default_args=default_args,
    schedule_interval="0 3 * * 0",   # weekly, Sunday 03:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lifecycle", "cleanup", "maintenance"],
    doc_md=__doc__,
) as dag:

    check_table_size = PythonOperator(
        task_id="check_table_size",
        python_callable=_check_table_size,
    )

    delete_old_metrics_in_batches = PythonOperator(
        task_id="delete_old_metrics_in_batches",
        python_callable=_delete_old_metrics_in_batches,
        execution_timeout=timedelta(hours=2),  # safety valve for very large deletes
    )

    vacuum_analyze_table = PythonOperator(
        task_id="vacuum_analyze_table",
        python_callable=_vacuum_analyze_table,
    )

    log_maintenance_record = PythonOperator(
        task_id="log_maintenance_record",
        python_callable=_log_maintenance_record,
    )

    check_table_size >> delete_old_metrics_in_batches >> vacuum_analyze_table >> log_maintenance_record
