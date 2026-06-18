"""
DAG 18: data_freshness_sla_tracker
Category: Monitoring & Alerting

WHAT IT DOES:
  Every hour, measures how "fresh" the data is at each stage of the pipeline:
    1. Source freshness: how old is the newest row in raw_events?
    2. Gold freshness: how old is the newest Parquet file in /data/gold/?
  If the gold layer is more than 2 hours behind source events, it's an SLA breach.

WHY WE USE IT:
  Data freshness is the most important SLA for an analytics platform.
  A 3-hour-old dashboard is worse than no dashboard — users make bad decisions.
  This DAG gives us an automated early warning before users notice stale data.

KEY AIRFLOW CONCEPT TAUGHT:
  SLA measurement pipeline pattern — building a DAG whose sole purpose is
  measuring pipeline latency (not processing data) and storing that measurement
  history. This is how production platforms maintain SLA compliance records.
  Also shows combining PythonOperator + BashOperator in the same DAG for
  tasks that need different runtimes (Python for DB queries, Bash for file stats).
"""

import asyncio
import glob
import os
import sys
import time
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
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_alert,
}

SLA_TARGET_SECONDS = 7200   # 2 hours — gold layer must be within 2h of source


def _measure_source_freshness(**context):
    """
    Finds the MAX(created_at) in raw_events and computes how many seconds
    ago that was. A large number means events have stopped flowing into Kafka/Postgres.
    """
    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            row = await conn.fetchrow(
                "SELECT MAX(created_at) AS latest, NOW() AS now FROM raw_events"
            )
        finally:
            await conn.close()
        return row

    result = asyncio.run(_query())
    latest = result["latest"]
    now = result["now"]

    if latest is None:
        lag_seconds = 999999
        print("No rows in raw_events — source is completely empty")
    else:
        lag_seconds = int((now - latest).total_seconds())
        print(f"Source freshness lag: {lag_seconds}s (latest event: {latest})")

    context["ti"].xcom_push(key="source_lag_seconds", value=lag_seconds)


def _measure_gold_freshness(**context):
    """
    Finds the most recently modified Parquet file in /data/gold/ using Python's
    os.stat() — no Bash needed for this. Computes seconds since last modification.

    If no gold files exist yet (first run / empty system), returns a high lag value
    rather than crashing — defensive programming in pipeline monitors is critical.
    """
    gold_files = glob.glob("/data/gold/**/*.parquet", recursive=True)

    if not gold_files:
        lag_seconds = 999999
        print("No Parquet files found in /data/gold/ — gold layer is empty")
    else:
        latest_mtime = max(os.stat(f).st_mtime for f in gold_files)
        lag_seconds = int(time.time() - latest_mtime)
        print(f"Gold freshness lag: {lag_seconds}s (latest file modified {lag_seconds}s ago)")

    context["ti"].xcom_push(key="gold_lag_seconds", value=lag_seconds)


def _compute_e2e_sla(**context):
    """
    Combines source and gold lag measurements to compute total end-to-end lag.
    The E2E lag is: "how long ago was the event that is now reflected in gold?"
    We approximate this as max(source_lag, gold_lag).
    """
    source_lag = context["ti"].xcom_pull(task_ids="measure_source_freshness", key="source_lag_seconds") or 0
    gold_lag = context["ti"].xcom_pull(task_ids="measure_gold_freshness", key="gold_lag_seconds") or 0

    e2e_lag = max(source_lag, gold_lag)
    sla_met = e2e_lag <= SLA_TARGET_SECONDS

    print(
        f"E2E lag: {e2e_lag}s | SLA target: {SLA_TARGET_SECONDS}s | "
        f"SLA {'MET' if sla_met else 'BREACHED'}"
    )

    context["ti"].xcom_push(key="e2e_lag_seconds", value=e2e_lag)
    context["ti"].xcom_push(key="sla_met", value=sla_met)


def _branch_sla(**context):
    sla_met = context["ti"].xcom_pull(task_ids="compute_e2e_sla", key="sla_met")
    return "record_sla_pass" if sla_met else "record_sla_breach"


def _record_sla_result(passed: bool, **context):
    """
    Writes the SLA measurement to sla_measurement_log regardless of whether
    SLA was met or breached. This builds a history we can query to compute
    weekly SLA compliance percentage.
    """
    e2e_lag = context["ti"].xcom_pull(task_ids="compute_e2e_sla", key="e2e_lag_seconds") or 0
    source_lag = context["ti"].xcom_pull(task_ids="measure_source_freshness", key="source_lag_seconds") or 0
    gold_lag = context["ti"].xcom_pull(task_ids="measure_gold_freshness", key="gold_lag_seconds") or 0

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                INSERT INTO sla_measurement_log
                    (measured_at, source_lag_seconds, gold_lag_seconds,
                     e2e_lag_seconds, sla_met, sla_target_seconds)
                VALUES (NOW(), $1, $2, $3, $4, $5)
                """,
                source_lag, gold_lag, e2e_lag, passed, SLA_TARGET_SECONDS,
            )
        finally:
            await conn.close()

    asyncio.run(_insert())


def _send_sla_breach_alert(**context):
    import requests

    e2e_lag = context["ti"].xcom_pull(task_ids="compute_e2e_sla", key="e2e_lag_seconds") or 0
    hours = e2e_lag // 3600
    minutes = (e2e_lag % 3600) // 60

    message = (
        f":fire: *SLA BREACH — Data Freshness*\n"
        f"*E2E pipeline lag:* `{hours}h {minutes}m`\n"
        f"*SLA target:* `{SLA_TARGET_SECONDS // 3600}h`\n"
        f"*Window:* `{context['data_interval_start']}`\n"
        f"Check: Is Kafka streaming consumer running? Is Airflow batch ETL succeeding?"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


with DAG(
    dag_id="data_freshness_sla_tracker",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["monitoring", "sla", "freshness"],
    doc_md=__doc__,
) as dag:

    ensure_sla_log_table = PostgresOperator(
        task_id="ensure_sla_log_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS sla_measurement_log (
                id                 BIGSERIAL PRIMARY KEY,
                measured_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                source_lag_seconds INTEGER NOT NULL,
                gold_lag_seconds   INTEGER NOT NULL,
                e2e_lag_seconds    INTEGER NOT NULL,
                sla_met            BOOLEAN NOT NULL,
                sla_target_seconds INTEGER NOT NULL
            );
        """,
    )

    measure_source_freshness = PythonOperator(
        task_id="measure_source_freshness",
        python_callable=_measure_source_freshness,
    )

    measure_gold_freshness = PythonOperator(
        task_id="measure_gold_freshness",
        python_callable=_measure_gold_freshness,
    )

    compute_e2e_sla = PythonOperator(
        task_id="compute_e2e_sla",
        python_callable=_compute_e2e_sla,
    )

    branch_sla = BranchPythonOperator(
        task_id="branch_sla",
        python_callable=_branch_sla,
    )

    record_sla_pass = PythonOperator(
        task_id="record_sla_pass",
        python_callable=_record_sla_result,
        op_kwargs={"passed": True},
    )

    record_sla_breach = PythonOperator(
        task_id="record_sla_breach",
        python_callable=_record_sla_result,
        op_kwargs={"passed": False},
    )

    send_sla_breach_alert = PythonOperator(
        task_id="send_sla_breach_alert",
        python_callable=_send_sla_breach_alert,
    )

    sla_check_done = EmptyOperator(
        task_id="sla_check_done",
        trigger_rule="none_failed_min_one_success",
    )

    ensure_sla_log_table >> [measure_source_freshness, measure_gold_freshness]
    [measure_source_freshness, measure_gold_freshness] >> compute_e2e_sla
    compute_e2e_sla >> branch_sla
    branch_sla >> record_sla_pass >> sla_check_done
    branch_sla >> record_sla_breach >> send_sla_breach_alert >> sla_check_done
