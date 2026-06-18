"""
DAG 17: spark_cluster_health_check
Category: Monitoring & Alerting

WHAT IT DOES:
  Every 10 minutes, polls the Spark master's REST API (http://spark-master:8080)
  to check: Is the master reachable? How many workers are alive? Are there any
  failed or stuck applications? Stores every health reading in cluster_health_log
  and alerts if workers drop below MIN_WORKERS.

WHY WE USE IT:
  Spark workers can crash silently — the batch ETL DAG will only fail when it
  tries to submit a job. By monitoring proactively, we detect worker loss
  between batch runs and can restart workers before the next scheduled job.

KEY AIRFLOW CONCEPT TAUGHT:
  PythonOperator with requests for internal service health checks — a common
  pattern in data platform operations. The key insight: Airflow tasks can
  make HTTP calls to any service in the same Docker network. This makes
  Airflow a universal control plane, not just a task scheduler.
  Also shows always-store-then-alert pattern: store the reading regardless
  of outcome so you have a complete health history.
"""

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_alert,
}

SPARK_MASTER_URL = "http://spark-master:8080"
MIN_WORKERS = 1  # alert if fewer than this many workers are alive


def _check_spark_master_api(**context):
    """
    Calls the Spark master REST API endpoints:
      /json/        → cluster overview (alive workers, cores, memory)
      /api/v1/applications → list of running/completed apps

    Uses a short timeout (5s) — if Spark is down, we want to fail fast
    and alert quickly, not wait 30 seconds per check.
    """
    import requests as req

    result = {
        "reachable": False,
        "alive_workers": 0,
        "total_cores": 0,
        "memory_used_mb": 0,
        "active_apps": 0,
        "error": None,
    }

    try:
        # Spark master JSON endpoint (simpler than the REST API for health checks)
        r = req.get(f"{SPARK_MASTER_URL}/json/", timeout=5)
        r.raise_for_status()
        data = r.json()

        result["reachable"] = True
        result["alive_workers"] = data.get("aliveworkers", 0)
        result["total_cores"] = data.get("cores", 0)
        result["memory_used_mb"] = data.get("memoryused", 0)

        # Also check active applications
        r2 = req.get(f"{SPARK_MASTER_URL}/api/v1/applications?status=running", timeout=5)
        if r2.status_code == 200:
            result["active_apps"] = len(r2.json())

    except req.exceptions.ConnectionError:
        result["error"] = "ConnectionError — Spark master is unreachable"
        print(f"ERROR: {result['error']}")
    except req.exceptions.Timeout:
        result["error"] = "Timeout — Spark master took >5s to respond"
        print(f"ERROR: {result['error']}")
    except Exception as e:
        result["error"] = str(e)
        print(f"ERROR: {result['error']}")

    print(f"Spark health check result: {result}")
    context["ti"].xcom_push(key="health", value=result)


def _evaluate_worker_count(**context):
    health = context["ti"].xcom_pull(task_ids="check_spark_master_api", key="health") or {}
    reachable = health.get("reachable", False)
    alive_workers = health.get("alive_workers", 0)

    if not reachable:
        return "cluster_unreachable_alert"
    if alive_workers < MIN_WORKERS:
        return "cluster_degraded_alert"
    return "cluster_healthy"


def _cluster_healthy(**context):
    health = context["ti"].xcom_pull(task_ids="check_spark_master_api", key="health") or {}
    print(
        f"Spark cluster HEALTHY: {health.get('alive_workers')} workers, "
        f"{health.get('total_cores')} cores, "
        f"{health.get('active_apps')} active apps"
    )


def _cluster_degraded_alert(**context):
    import requests as req

    health = context["ti"].xcom_pull(task_ids="check_spark_master_api", key="health") or {}
    message = (
        f":warning: *Spark Cluster Degraded*\n"
        f"*Alive workers:* `{health.get('alive_workers', 0)}` (minimum: {MIN_WORKERS})\n"
        f"*Total cores:* `{health.get('total_cores', 0)}`\n"
        f"*Action:* Run `docker compose restart spark-worker-1 spark-worker-2`"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                req.post(url, json={key: message}, timeout=5)
            except req.RequestException:
                pass


def _cluster_unreachable_alert(**context):
    import requests as req

    health = context["ti"].xcom_pull(task_ids="check_spark_master_api", key="health") or {}
    message = (
        f":red_circle: *Spark Master Unreachable*\n"
        f"*URL:* `{SPARK_MASTER_URL}`\n"
        f"*Error:* `{health.get('error', 'unknown')}`\n"
        f"*Action:* Run `docker compose ps spark-master` and check logs"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                req.post(url, json={key: message}, timeout=5)
            except req.RequestException:
                pass


def _store_health_event(**context):
    """
    ALWAYS stores the health reading — both when healthy and degraded.
    This is the "always record, then alert" pattern. With a full history
    in cluster_health_log, we can compute uptime percentage and spot
    patterns (e.g., workers dying every Monday morning after weekend restarts).
    """
    import asyncio
    import asyncpg

    health = context["ti"].xcom_pull(task_ids="check_spark_master_api", key="health") or {}
    reachable = health.get("reachable", False)
    alive_workers = health.get("alive_workers", 0)
    status = "ok" if (reachable and alive_workers >= MIN_WORKERS) else "degraded"

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
                VALUES ('spark', $1, $2::jsonb)
                """,
                status,
                json.dumps(health),
            )
        finally:
            await conn.close()

    asyncio.run(_insert())


with DAG(
    dag_id="spark_cluster_health_check",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["monitoring", "spark", "health"],
    doc_md=__doc__,
) as dag:

    check_spark_master_api = PythonOperator(
        task_id="check_spark_master_api",
        python_callable=_check_spark_master_api,
    )

    evaluate_worker_count = BranchPythonOperator(
        task_id="evaluate_worker_count",
        python_callable=_evaluate_worker_count,
    )

    cluster_healthy = PythonOperator(
        task_id="cluster_healthy",
        python_callable=_cluster_healthy,
    )

    cluster_degraded_alert = PythonOperator(
        task_id="cluster_degraded_alert",
        python_callable=_cluster_degraded_alert,
    )

    cluster_unreachable_alert = PythonOperator(
        task_id="cluster_unreachable_alert",
        python_callable=_cluster_unreachable_alert,
    )

    # Runs regardless of which branch — always store the health reading
    store_health_event = PythonOperator(
        task_id="store_health_event",
        python_callable=_store_health_event,
        trigger_rule="none_failed_min_one_success",
    )

    check_spark_master_api >> evaluate_worker_count
    evaluate_worker_count >> cluster_healthy >> store_health_event
    evaluate_worker_count >> cluster_degraded_alert >> store_health_event
    evaluate_worker_count >> cluster_unreachable_alert >> store_health_event
