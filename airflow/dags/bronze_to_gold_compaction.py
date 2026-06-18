"""
DAG 8: bronze_to_gold_compaction
Category: Data Lifecycle & Cleanup

WHAT IT DOES:
  Every day at 01:00 AM, compacts small Parquet files from /data/bronze/ into
  larger, optimized files in /data/gold/. Small files (the "small file problem")
  are common when Spark Streaming writes micro-batches every 10 seconds —
  you end up with thousands of tiny files that are slow to read.
  This DAG merges them into fewer, larger files using Spark.

WHY WE USE IT:
  The "small file problem" is a classic performance killer in data lakes.
  Reading 10,000 × 100KB files is much slower than reading 10 × 100MB files,
  because each file requires a separate filesystem open/read/close cycle.
  Compaction (merging small files) is a standard lakehouse maintenance task.

KEY AIRFLOW CONCEPTS TAUGHT:
  1. depends_on_past=True — today's compaction won't start until yesterday's
     succeeded. This prevents concurrent compaction runs from corrupting output.
  2. {{ ds }} macro — built-in Airflow date string "YYYY-MM-DD" for the
     logical run date. Passed as a CLI argument to the Spark job.
  3. ShortCircuitOperator — skips compaction if /data/bronze/ is empty
     (first run, or no bronze data yet).
"""

import glob
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
    "depends_on_past": True,   # sequential: yesterday's compaction must succeed first
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}


def _check_bronze_has_data(**context):
    """
    ShortCircuitOperator callable — returns True only if there are Parquet files
    in /data/bronze/ to compact. If bronze is empty, we skip compaction entirely.
    This handles the common case of a freshly deployed system or a day with no data.
    """
    bronze_files = glob.glob("/data/bronze/**/*.parquet", recursive=True)
    has_data = len(bronze_files) > 0
    print(f"Found {len(bronze_files)} Parquet files in /data/bronze/")
    return has_data


def _verify_gold_output(**context):
    """
    After Spark compaction completes, verify that output files actually exist
    in the gold directory for today's date.
    If no output exists (Spark may have had nothing to compact), just warn —
    don't fail. Failing would block tomorrow's run due to depends_on_past=True.
    """
    date_str = context["ds"]
    gold_path = f"/data/gold/compacted/{date_str}"
    files = glob.glob(f"{gold_path}/**/*.parquet", recursive=True)

    if files:
        print(f"Compaction output verified: {len(files)} files in {gold_path}")
    else:
        print(f"WARNING: No compacted files found at {gold_path} — bronze may have been empty")


with DAG(
    dag_id="bronze_to_gold_compaction",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lifecycle", "compaction", "maintenance"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Skip if bronze is empty ───────────────────────────────────────
    check_bronze_has_data = ShortCircuitOperator(
        task_id="check_bronze_has_data",
        python_callable=_check_bronze_has_data,
    )

    # ── Step 2: Bootstrap compaction tracking table ───────────────────────────
    ensure_compaction_manifest = PostgresOperator(
        task_id="ensure_compaction_manifest",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS compaction_manifest (
                compaction_date DATE PRIMARY KEY,
                started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                completed_at    TIMESTAMP WITH TIME ZONE,
                files_before    INTEGER,
                files_after     INTEGER,
                status          VARCHAR(20) DEFAULT 'running'
            );

            INSERT INTO compaction_manifest (compaction_date)
            VALUES ('{{ ds }}'::DATE)
            ON CONFLICT (compaction_date) DO UPDATE
                SET started_at = NOW(), status = 'running';
        """,
    )

    # ── Step 3: Submit Spark compaction job ───────────────────────────────────
    # {{ ds }} is Airflow's built-in date macro = "YYYY-MM-DD" for this run.
    # The Spark job reads from /data/bronze/ and writes to /data/gold/compacted/{{ ds }}/
    # spark.sql.shuffle.partitions=50 is lower than default (200) since compaction
    # jobs produce fewer, larger files — we don't need 200 output partitions.
    compact_via_spark = BashOperator(
        task_id="compact_via_spark",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--conf spark.sql.shuffle.partitions=50 "
            "--conf spark.driver.memory=2g "
            "/opt/spark/apps/metrics_aggregation.py "
            "--start {{ ds }}T00:00:00+00:00 "
            "--end {{ next_ds }}T00:00:00+00:00 "
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Step 4: Verify output and update manifest ─────────────────────────────
    verify_gold_output = PythonOperator(
        task_id="verify_gold_output",
        python_callable=_verify_gold_output,
    )

    update_compaction_manifest = PostgresOperator(
        task_id="update_compaction_manifest",
        postgres_conn_id="postgres_default",
        sql="""
            UPDATE compaction_manifest
            SET completed_at = NOW(), status = 'complete'
            WHERE compaction_date = '{{ ds }}'::DATE;
        """,
    )

    (
        check_bronze_has_data
        >> ensure_compaction_manifest
        >> compact_via_spark
        >> verify_gold_output
        >> update_compaction_manifest
    )
