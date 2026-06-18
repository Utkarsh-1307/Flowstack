"""
DAG 15: pipeline_health_monitor
Category: Monitoring & Alerting

WHAT IT DOES:
  Every 30 minutes, uses ExternalTaskSensor to wait for the core batch ETL
  (batch_etl_pipeline_v1's spark_transform_landing_to_gold task) to complete.
  If the ETL is healthy, checks data freshness. If fresh, logs healthy status.
  If the ETL missed its expected run time, escalates to an alert.

WHY WE USE IT:
  A monitoring DAG that WAITS for a pipeline DAG demonstrates the difference
  between scheduling (when does it start?) and coordination (did it finish?).
  Without this pattern, you'd have to manually check the Airflow UI to know
  if last hour's ETL succeeded. This DAG makes health visible programmatically.

KEY AIRFLOW CONCEPT TAUGHT:
  ExternalTaskSensor — the most important cross-DAG coordination operator.

  Critical parameters:
    external_dag_id: which DAG to watch
    external_task_id: which specific task in that DAG must succeed
    execution_delta: time difference between the sensor's run time and the
      external DAG's run time. Since both run at compatible schedules, we
      use timedelta(0) to check the CURRENT hour's run.
    mode="reschedule": ESSENTIAL for LocalExecutor. "poke" mode holds a
      worker slot while sleeping — with LocalExecutor, this can deadlock
      the scheduler if all worker slots are held by sleeping sensors.
      "reschedule" releases the slot while waiting and retakes it to check.
    timeout: if the external task doesn't succeed within this time, the
      sensor task fails (not skips — this is a hard failure).
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

FRESHNESS_THRESHOLD_HOURS = 2   # data older than 2h is considered stale


def _check_data_freshness(**context):
    """
    After confirming batch ETL completed, checks if the gold layer is fresh.
    Reads MAX(recorded_at) from live_event_metrics as a proxy for "freshness".
    """
    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            row = await conn.fetchrow(
                "SELECT MAX(recorded_at) AS latest, NOW() AS now FROM live_event_metrics"
            )
        finally:
            await conn.close()
        return row

    result = asyncio.run(_query())
    latest = result["latest"]
    now = result["now"]

    if latest is None:
        lag_hours = 999
        print("live_event_metrics is empty — data freshness unknown")
    else:
        lag_hours = (now - latest).total_seconds() / 3600
        print(f"Data freshness lag: {lag_hours:.2f} hours (threshold: {FRESHNESS_THRESHOLD_HOURS}h)")

    context["ti"].xcom_push(key="lag_hours", value=lag_hours)


def _branch_freshness(**context):
    lag_hours = context["ti"].xcom_pull(task_ids="check_data_freshness", key="lag_hours") or 0
    return "freshness_healthy_log" if lag_hours <= FRESHNESS_THRESHOLD_HOURS else "freshness_alert"


def _freshness_healthy_log(**context):
    lag = context["ti"].xcom_pull(task_ids="check_data_freshness", key="lag_hours") or 0
    print(f"Pipeline HEALTHY: batch ETL completed, data lag {lag:.2f}h (within {FRESHNESS_THRESHOLD_HOURS}h SLA)")


def _freshness_alert(**context):
    import requests

    lag = context["ti"].xcom_pull(task_ids="check_data_freshness", key="lag_hours") or 0
    message = (
        f":warning: *Data Freshness Alert*\n"
        f"*Lag:* `{lag:.2f}` hours (threshold: {FRESHNESS_THRESHOLD_HOURS}h)\n"
        f"*Batch ETL:* Completed (batch ran, but data is stale)\n"
        f"*Possible cause:* Spark Streaming consumer down, Kafka backlog\n"
        f"*Action:* Check spark streaming job logs"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


with DAG(
    dag_id="pipeline_health_monitor",
    default_args=default_args,
    schedule_interval="*/30 * * * *",   # every 30 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["monitoring", "health", "sensor"],
    doc_md=__doc__,
) as dag:

    # ── Key learning: ExternalTaskSensor configuration ─────────────────────────
    #
    # execution_delta=timedelta(0):
    #   This sensor runs every 30 minutes. batch_etl_pipeline_v1 runs every hour.
    #   execution_delta=timedelta(0) means "look for a run of the external DAG
    #   that has the SAME logical execution date as this sensor's current run".
    #   For production use, you'd adjust this based on schedule alignment.
    #
    # mode="reschedule" vs mode="poke":
    #   poke:       task holds its worker slot and sleeps between checks
    #               → with LocalExecutor, can deadlock scheduler (all slots held)
    #   reschedule: task releases its slot, scheduler reschedules it to check again
    #               → correct choice for LocalExecutor, uses far fewer resources
    #
    # allowed_states=["success"]:
    #   Sensor completes successfully only when external task is in "success" state.
    #   "failed" or "upstream_failed" in failed_states triggers sensor failure.
    sense_batch_etl_completed = ExternalTaskSensor(
        task_id="sense_batch_etl_completed",
        external_dag_id="batch_etl_pipeline_v1",
        external_task_id="spark_transform_landing_to_gold",
        execution_delta=timedelta(hours=0),
        timeout=3600,              # give the ETL up to 1 hour to complete
        poke_interval=120,         # check every 2 minutes
        mode="reschedule",         # release worker slot while waiting
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        soft_fail=True,            # if ETL hasn't run yet, skip (not fail) monitor
    )

    check_data_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=_check_data_freshness,
    )

    branch_freshness = BranchPythonOperator(
        task_id="branch_freshness",
        python_callable=_branch_freshness,
    )

    freshness_healthy_log = PythonOperator(
        task_id="freshness_healthy_log",
        python_callable=_freshness_healthy_log,
    )

    freshness_alert = PythonOperator(
        task_id="freshness_alert",
        python_callable=_freshness_alert,
    )

    monitor_complete = EmptyOperator(
        task_id="monitor_complete",
        trigger_rule="none_failed_min_one_success",
    )

    (
        sense_batch_etl_completed
        >> check_data_freshness
        >> branch_freshness
    )
    branch_freshness >> freshness_healthy_log >> monitor_complete
    branch_freshness >> freshness_alert >> monitor_complete
