"""
DAG 2: dq_schema_drift_detector
Category: Data Quality & Validation

WHAT IT DOES:
  Every 2 hours, finds the most recent Parquet file in the landing zone and
  compares its column names/types against a reference schema. If the schema
  has changed (a column renamed, type changed, column added/removed), it logs
  a drift record and sends an alert. If no Parquet files exist yet, it skips
  all downstream tasks — no noise when the system is empty.

WHY WE USE IT:
  Schema drift is one of the most common silent failures in data pipelines.
  An upstream service adds a field or changes a type, and your Spark jobs
  start failing cryptically. Detecting it at the landing zone (immediately
  after extraction) means you catch it before it cascades downstream.

KEY AIRFLOW CONCEPT TAUGHT:
  ShortCircuitOperator — unlike BranchPythonOperator (which picks a branch),
  ShortCircuitOperator returns a boolean. If False, ALL downstream tasks are
  marked SKIPPED (not FAILED). This is the right pattern when "no data" is
  a normal, expected state — you want zero noise, not a red alert.
"""

import glob
import os
import sys
from datetime import datetime, timedelta

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

# The expected schema: column name → expected PyArrow type string
REFERENCE_SCHEMA = {
    "id": "int64",
    "user_id": "string",
    "event_type": "string",
    "payload": "string",
    "created_at": "timestamp[us, tz=UTC]",
}


def _check_landing_has_files(**context):
    """
    ShortCircuitOperator callable — MUST return bool.
    Returns True if there are any Parquet files in the landing zone.
    Returns False to skip all downstream tasks silently.

    Why not raise an exception? Because "no files" is not an error state —
    it's normal during the first few hours of deployment or during low traffic.
    ShortCircuitOperator gives us clean SKIPPED states instead of red FAILEDs.
    """
    parquet_files = glob.glob("/data/landing/**/*.parquet", recursive=True)
    has_files = len(parquet_files) > 0
    print(f"Found {len(parquet_files)} Parquet files in landing zone")

    if not has_files:
        print("No files found — skipping schema validation (this is normal on first runs)")
    return has_files


def _validate_schema(**context):
    """
    Reads the actual schema from the most recently modified Parquet file
    and compares it field-by-field against REFERENCE_SCHEMA.

    Uses pyarrow.parquet.read_schema() — this reads ONLY the schema metadata
    (footer of the Parquet file), not the data itself. Extremely fast even
    for multi-GB files.
    """
    import pyarrow.parquet as pq

    parquet_files = glob.glob("/data/landing/**/*.parquet", recursive=True)
    # Use the most recently written file (closest to "live" schema)
    latest_file = max(parquet_files, key=os.path.getmtime)
    print(f"Checking schema of: {latest_file}")

    schema = pq.read_schema(latest_file)
    actual_schema = {field.name: str(field.type) for field in schema}
    print(f"Actual schema:    {actual_schema}")
    print(f"Reference schema: {REFERENCE_SCHEMA}")

    missing_cols = set(REFERENCE_SCHEMA.keys()) - set(actual_schema.keys())
    extra_cols = set(actual_schema.keys()) - set(REFERENCE_SCHEMA.keys())
    type_mismatches = {
        col: {"expected": REFERENCE_SCHEMA[col], "actual": actual_schema[col]}
        for col in REFERENCE_SCHEMA.keys() & actual_schema.keys()
        if REFERENCE_SCHEMA[col] != actual_schema[col]
    }

    drift_detected = bool(missing_cols or extra_cols or type_mismatches)
    drift_details = {
        "missing_columns": list(missing_cols),
        "extra_columns": list(extra_cols),
        "type_mismatches": type_mismatches,
        "checked_file": latest_file,
    }

    context["ti"].xcom_push(key="drift_detected", value=drift_detected)
    context["ti"].xcom_push(key="drift_details", value=drift_details)

    if drift_detected:
        print(f"SCHEMA DRIFT DETECTED: {drift_details}")
    else:
        print("Schema is valid — no drift detected")


def _handle_drift_result(**context):
    """
    Runs regardless of whether drift was detected (trigger_rule handles this).
    Writes the result to dq_schema_drift_log and sends an alert if drift found.
    """
    import asyncio
    import asyncpg
    import requests

    drift_detected = context["ti"].xcom_pull(task_ids="validate_schema", key="drift_detected")
    drift_details = context["ti"].xcom_pull(task_ids="validate_schema", key="drift_details") or {}

    # Always write the audit record
    async def _log():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dq_schema_drift_log (
                    id             BIGSERIAL PRIMARY KEY,
                    checked_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    drift_detected BOOLEAN NOT NULL,
                    missing_cols   TEXT[],
                    extra_cols     TEXT[],
                    type_mismatches JSONB
                );

                INSERT INTO dq_schema_drift_log
                    (drift_detected, missing_cols, extra_cols, type_mismatches)
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                drift_detected,
                drift_details.get("missing_columns", []),
                drift_details.get("extra_columns", []),
                str(drift_details.get("type_mismatches", {})),
            )
        finally:
            await conn.close()

    asyncio.run(_log())

    if drift_detected:
        message = (
            f":warning: *Schema Drift Detected in Landing Zone*\n"
            f"*File checked:* `{drift_details.get('checked_file', 'unknown')}`\n"
            f"*Missing columns:* `{drift_details.get('missing_columns', [])}`\n"
            f"*Extra columns:* `{drift_details.get('extra_columns', [])}`\n"
            f"*Type mismatches:* `{drift_details.get('type_mismatches', {})}`"
        )
        for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
            url = os.getenv(url_env, "")
            if url:
                try:
                    requests.post(url, json={key: message}, timeout=5)
                except requests.RequestException:
                    pass


with DAG(
    dag_id="dq_schema_drift_detector",
    default_args=default_args,
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-quality", "schema", "validation"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Short-circuit if no files exist ────────────────────────────────
    # If this returns False, validate_schema and handle_drift_result are SKIPPED.
    # The DAG run shows as SUCCESS with some tasks SKIPPED — clean and correct.
    check_landing_has_files = ShortCircuitOperator(
        task_id="check_landing_has_files",
        python_callable=_check_landing_has_files,
    )

    # ── Step 2: Read and compare actual vs reference schema ───────────────────
    validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=_validate_schema,
    )

    # ── Step 3: Log result and alert if drifted ───────────────────────────────
    handle_drift_result = PythonOperator(
        task_id="handle_drift_result",
        python_callable=_handle_drift_result,
    )

    check_landing_has_files >> validate_schema >> handle_drift_result
