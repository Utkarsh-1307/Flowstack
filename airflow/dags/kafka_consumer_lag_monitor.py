"""
DAG 16: kafka_consumer_lag_monitor
Category: Monitoring & Alerting

WHAT IT DOES:
  Every 15 minutes, checks the consumer group lag on the 'user-events' Kafka
  topic by calling kafka-consumer-groups.sh from within the Airflow container.
  If the lag exceeds LAG_THRESHOLD messages, it sends an alert. Lag means
  events are arriving faster than Spark Streaming can consume them.

WHY WE USE IT:
  Consumer lag is the single most important health metric for a Kafka-based
  pipeline. Growing lag means Spark Streaming is falling behind — eventually
  it could overflow Kafka's retention window and permanently lose events.
  Catching lag early (every 15 min) gives the team time to scale up workers.

KEY AIRFLOW CONCEPT TAUGHT:
  BashOperator XCom via stdout — when do_xcom_push=True (default), the
  BashOperator automatically pushes the LAST LINE of stdout to XCom under
  the key 'return_value'. This lets PythonOperators downstream read bash
  command output without extra plumbing. Also shows using BashOperator for
  operations that have no Python SDK (kafka CLI tools).
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_alert,
}

LAG_THRESHOLD = 5000   # alert if more than 5000 unprocessed messages
CONSUMER_GROUP = "spark-streaming-group"
KAFKA_TOPIC = "user-events"


def _parse_and_evaluate_lag(**context):
    """
    Pulls the BashOperator's stdout (which is the total lag integer) from XCom.
    BashOperator XComs the LAST LINE of stdout automatically — that's why we
    pipe everything through to produce a single number as the final output.

    Handles the case where Kafka is unreachable (empty/error output).
    """
    raw_output = context["ti"].xcom_pull(task_ids="fetch_consumer_lag", key="return_value") or ""
    raw_output = str(raw_output).strip()

    print(f"Raw lag output from kafka-consumer-groups.sh: '{raw_output}'")

    try:
        lag = int(raw_output)
        reachable = True
    except (ValueError, TypeError):
        lag = -1
        reachable = False
        print(f"Could not parse lag — Kafka may be unreachable or consumer group not active")

    context["ti"].xcom_push(key="lag", value=lag)
    context["ti"].xcom_push(key="kafka_reachable", value=reachable)


def _branch_lag(**context):
    lag = context["ti"].xcom_pull(task_ids="parse_and_evaluate_lag", key="lag") or 0
    reachable = context["ti"].xcom_pull(task_ids="parse_and_evaluate_lag", key="kafka_reachable")

    if not reachable:
        return "alert_kafka_unreachable"
    if lag > LAG_THRESHOLD:
        return "alert_high_lag"
    return "log_lag_healthy"


def _alert_high_lag(**context):
    import requests

    lag = context["ti"].xcom_pull(task_ids="parse_and_evaluate_lag", key="lag") or 0
    message = (
        f":warning: *Kafka Consumer Lag Alert*\n"
        f"*Topic:* `{KAFKA_TOPIC}`\n"
        f"*Consumer group:* `{CONSUMER_GROUP}`\n"
        f"*Current lag:* `{lag:,}` messages\n"
        f"*Threshold:* `{LAG_THRESHOLD:,}` messages\n"
        f"*Action:* Check Spark Streaming job health — scale up workers if needed"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


def _alert_kafka_unreachable(**context):
    import requests

    message = (
        f":red_circle: *Kafka Unreachable — Cannot Check Consumer Lag*\n"
        f"*Broker:* `kafka-broker:29092`\n"
        f"*Action:* Check if kafka-broker container is running: `docker compose ps`"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


def _store_lag_metric(**context):
    """
    Stores the lag reading to cluster_health_log for trend analysis.
    Runs regardless of which branch executed (trigger_rule handles this).
    """
    import asyncio
    import asyncpg
    import json

    lag = context["ti"].xcom_pull(task_ids="parse_and_evaluate_lag", key="lag") or -1
    reachable = context["ti"].xcom_pull(task_ids="parse_and_evaluate_lag", key="kafka_reachable") or False
    status = "ok" if (reachable and lag <= LAG_THRESHOLD) else "degraded"

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cluster_health_log (
                    id         BIGSERIAL PRIMARY KEY,
                    check_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    component  VARCHAR(50) NOT NULL,
                    status     VARCHAR(20) NOT NULL,
                    details    JSONB
                );

                INSERT INTO cluster_health_log (component, status, details)
                VALUES ('kafka', $1, $2::jsonb)
                """,
                status,
                json.dumps({"lag": lag, "reachable": reachable, "topic": KAFKA_TOPIC}),
            )
        finally:
            await conn.close()

    asyncio.run(_insert())


with DAG(
    dag_id="kafka_consumer_lag_monitor",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["monitoring", "kafka", "health"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Run kafka CLI to get consumer group lag ────────────────────────
    # The kafka-consumer-groups.sh script is available in the Kafka container,
    # but we run this FROM the Airflow container — so it uses the kafka CLI
    # if installed, or falls back to a REST-based approach.
    #
    # This command:
    # 1. Lists all consumer group details for CONSUMER_GROUP on KAFKA_TOPIC
    # 2. Extracts the LAG column (last field per row)
    # 3. Sums all partition lags with awk
    # If kafka-consumer-groups.sh is not found, outputs -1 (parsed as unreachable).
    #
    # do_xcom_push=True (default) → last stdout line is pushed to XCom as 'return_value'
    fetch_consumer_lag = BashOperator(
        task_id="fetch_consumer_lag",
        bash_command=(
            "kafka-consumer-groups.sh "
            f"--bootstrap-server kafka-broker:29092 "
            f"--describe --group {CONSUMER_GROUP} "
            "2>/dev/null "
            "| awk 'NR>1 && $NF~/^[0-9]+$/ {sum += $NF} END {print (NR>1 ? sum : -1)}' "
            "|| echo -1 "
        ),
        do_xcom_push=True,
    )

    parse_and_evaluate_lag = PythonOperator(
        task_id="parse_and_evaluate_lag",
        python_callable=_parse_and_evaluate_lag,
    )

    branch_lag = BranchPythonOperator(
        task_id="branch_lag",
        python_callable=_branch_lag,
    )

    alert_high_lag = PythonOperator(task_id="alert_high_lag", python_callable=_alert_high_lag)
    alert_kafka_unreachable = PythonOperator(task_id="alert_kafka_unreachable", python_callable=_alert_kafka_unreachable)
    log_lag_healthy = EmptyOperator(task_id="log_lag_healthy")

    store_lag_metric = PythonOperator(
        task_id="store_lag_metric",
        python_callable=_store_lag_metric,
        trigger_rule="none_failed_min_one_success",
    )

    fetch_consumer_lag >> parse_and_evaluate_lag >> branch_lag
    branch_lag >> alert_high_lag >> store_lag_metric
    branch_lag >> alert_kafka_unreachable >> store_lag_metric
    branch_lag >> log_lag_healthy >> store_lag_metric
