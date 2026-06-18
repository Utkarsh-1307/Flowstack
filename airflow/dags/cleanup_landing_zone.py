"""
DAG 6: cleanup_landing_zone
Category: Data Lifecycle & Cleanup

WHAT IT DOES:
  Every day at 02:00 AM, deletes Parquet files from /data/landing/ that are
  older than RETENTION_DAYS (default: 30). Also deletes empty directories
  left behind after file removal. Logs cleanup stats (files deleted, MB freed)
  to pipeline_run_log.

WHY WE USE IT:
  The landing zone grows forever if left uncleaned — every hourly batch
  adds a new Parquet file. At ~1MB per file and 24 files/day, that's ~720MB/month.
  On a laptop or small VM, this fills disk in weeks. Automated retention
  cleanup is essential for sustainable pipeline operation.

KEY AIRFLOW CONCEPT TAUGHT:
  BashOperator with environment variable fallback — reads LANDING_RETENTION_DAYS
  from the environment (set in .env), falling back to 30 days if not set.
  Also shows BashOperator XCom: the last line of stdout is automatically pushed
  to XCom as 'return_value', which the next PythonOperator can pull and parse.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
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

DEFAULT_RETENTION_DAYS = 30


def _check_landing_zone_exists(**context):
    """
    ShortCircuitOperator callable — returns False if /data/landing/ doesn't
    exist or is completely empty, skipping all cleanup tasks.
    This prevents errors on a freshly deployed system with no data yet.
    """
    import glob

    has_files = len(glob.glob("/data/landing/**/*.parquet", recursive=True)) > 0
    print(f"Landing zone has Parquet files: {has_files}")
    return has_files


def _log_cleanup_stats(**context):
    """
    Pulls the disk usage reading from BashOperator XCom and logs a cleanup
    audit record. The BashOperator XComs the LAST LINE of its stdout.
    """
    import asyncio
    import asyncpg

    # XCom from check_disk_usage BashOperator (last line of stdout)
    disk_usage = context["ti"].xcom_pull(task_ids="check_disk_usage", key="return_value") or "unknown"
    print(f"Landing zone disk usage before cleanup: {disk_usage}")

    async def _log():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                INSERT INTO pipeline_run_log (dag_id, run_date, row_count, mode, passed_at)
                VALUES ($1, $2::date, 0, $3, NOW())
                ON CONFLICT (dag_id, run_date) DO UPDATE
                    SET mode = EXCLUDED.mode, passed_at = NOW()
                """,
                "cleanup_landing_zone",
                context["ds"],
                f"cleanup:before={disk_usage}",
            )
        finally:
            await conn.close()

    asyncio.run(_log())


with DAG(
    dag_id="cleanup_landing_zone",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lifecycle", "cleanup", "maintenance"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Skip if nothing to clean ──────────────────────────────────────
    check_landing_zone_exists = ShortCircuitOperator(
        task_id="check_landing_zone_exists",
        python_callable=_check_landing_zone_exists,
    )

    # ── Step 2: Measure disk usage before cleanup (for audit logging) ─────────
    # du -sh outputs "42M  /data/landing/" — we take the first field (the size)
    # The last line of stdout is XCom'd automatically as 'return_value'
    check_disk_usage = BashOperator(
        task_id="check_disk_usage",
        bash_command="du -sh /data/landing/ 2>/dev/null | awk '{print $1}' ",
        do_xcom_push=True,
    )

    # ── Step 3: Delete old Parquet files ──────────────────────────────────────
    # Reads retention days from the LANDING_RETENTION_DAYS environment variable,
    # falling back to DEFAULT_RETENTION_DAYS if not set.
    # To override: set LANDING_RETENTION_DAYS=60 in .env and restart containers.
    # find -mtime +N means "modified more than N days ago"
    delete_old_landing_files = BashOperator(
        task_id="delete_old_landing_files",
        bash_command=(
            f"RETENTION_DAYS=${{LANDING_RETENTION_DAYS:-{DEFAULT_RETENTION_DAYS}}} && "
            "echo \"Deleting files older than $RETENTION_DAYS days from /data/landing/\" && "
            "find /data/landing/ -name '*.parquet' -mtime +$RETENTION_DAYS -type f -delete && "
            "find /data/landing/ -type d -empty -delete && "
            "echo 'Cleanup complete' "
        ),
    )

    # ── Step 4: Log cleanup audit ─────────────────────────────────────────────
    log_cleanup_stats = PythonOperator(
        task_id="log_cleanup_stats",
        python_callable=_log_cleanup_stats,
    )

    check_landing_zone_exists >> check_disk_usage >> delete_old_landing_files >> log_cleanup_stats
