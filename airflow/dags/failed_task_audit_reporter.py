"""
DAG 20: failed_task_audit_reporter
Category: DLQ & Error Handling

WHAT IT DOES:
  Every day at 07:00, queries the Airflow metadata database's task_instance
  table to find all failed tasks in the past 24 hours. Groups them by DAG
  and counts repeated failures. Writes a failure summary to pipeline_failure_audit
  in the analytics database. Sends an escalation alert if failure rate is high.

WHY WE USE IT:
  Without this DAG, detecting systemic failures requires manually browsing the
  Airflow UI. This DAG makes the Airflow metadata database itself a data source —
  a powerful pattern. Instead of looking at the UI, you query failure patterns
  just like any other data, enabling automated escalation and trend analysis.

KEY AIRFLOW CONCEPT TAUGHT:
  Airflow's metadata database is queryable — the task_instance, dag_run, and
  dag tables contain rich operational data about every DAG run ever executed.
  Querying them in a PythonOperator gives you a programmable audit system.
  This is an advanced pattern that most engineers don't know about.

  The Airflow metadata DB URL is available via environment variable
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN (set by Airflow itself).
"""

import asyncio
import os
import sys
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
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

FAILURE_RATE_ESCALATION_THRESHOLD = 5  # escalate if any DAG has > 5 failures in 24h


def _get_airflow_meta_db_url():
    """
    Gets the Airflow metadata DB connection string.
    Airflow sets AIRFLOW__DATABASE__SQL_ALCHEMY_CONN automatically.
    We strip the dialect prefix to get a plain asyncpg-compatible URL.
    """
    conn_str = os.getenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        os.getenv("DATABASE_URL", ""),  # fallback to app DB
    )
    # Remove SQLAlchemy dialect prefixes
    for prefix in ["postgresql+psycopg2://", "postgresql+asyncpg://", "postgresql://"]:
        if conn_str.startswith(prefix):
            return "postgresql://" + conn_str[len(prefix):]
    return conn_str.replace("+asyncpg", "").replace("+psycopg2", "")


def _query_airflow_metadata_for_failures(**context):
    """
    Reads from Airflow's OWN task_instance table to get failures.

    The task_instance table has one row per (dag_id, task_id, execution_date, try_number).
    STATE = 'failed' means the task reached its final failed state (all retries exhausted).
    STATE = 'up_for_retry' means it's about to be retried.
    We look at 'failed' only — these are the definitive failures.

    This is READ-ONLY on the metadata DB — perfectly safe even while Airflow runs.
    """
    async def _query():
        meta_url = _get_airflow_meta_db_url()
        try:
            conn = await asyncpg.connect(meta_url)
        except Exception as e:
            print(f"Could not connect to Airflow metadata DB: {e}")
            return []

        try:
            rows = await conn.fetch(
                """
                SELECT
                    dag_id,
                    task_id,
                    state,
                    start_date,
                    end_date,
                    try_number,
                    max_tries,
                    hostname
                FROM task_instance
                WHERE state = 'failed'
                  AND start_date >= NOW() - INTERVAL '24 hours'
                ORDER BY start_date DESC
                LIMIT 1000
                """
            )
        finally:
            await conn.close()
        return [dict(r) for r in rows]

    failures = asyncio.run(_query())
    print(f"Found {len(failures)} failed task instances in the past 24 hours")
    context["ti"].xcom_push(key="failure_count", value=len(failures))
    context["ti"].xcom_push(key="failures", value=failures[:100])  # cap XCom size


def _analyze_failure_patterns(**context):
    """
    Groups failures by DAG ID and counts them. Identifies:
    - Which DAGs have the most failures
    - Repeated failures (same task failing multiple times)
    - Systemic issues (many DAGs failing at the same time)
    """
    failures = context["ti"].xcom_pull(
        task_ids="query_airflow_metadata_for_failures", key="failures"
    ) or []

    # Count failures per DAG
    by_dag = {}
    for f in failures:
        dag_id = f["dag_id"]
        if dag_id not in by_dag:
            by_dag[dag_id] = {"count": 0, "tasks": set()}
        by_dag[dag_id]["count"] += 1
        by_dag[dag_id]["tasks"].add(f["task_id"])

    # Convert sets to lists for JSON serialization
    for dag_id in by_dag:
        by_dag[dag_id]["tasks"] = list(by_dag[dag_id]["tasks"])

    worst_offender = max(by_dag.items(), key=lambda x: x[1]["count"]) if by_dag else None
    max_failures = worst_offender[1]["count"] if worst_offender else 0

    print(f"Failure analysis: {by_dag}")
    print(f"Worst offender: {worst_offender}")

    context["ti"].xcom_push(key="by_dag", value=by_dag)
    context["ti"].xcom_push(key="max_failures", value=max_failures)


def _branch_failure_rate(**context):
    max_failures = context["ti"].xcom_pull(task_ids="analyze_failure_patterns", key="max_failures") or 0
    if max_failures > FAILURE_RATE_ESCALATION_THRESHOLD:
        return "send_escalation_alert"
    return "send_ok_summary"


def _send_escalation_alert(**context):
    import requests

    by_dag = context["ti"].xcom_pull(task_ids="analyze_failure_patterns", key="by_dag") or {}
    total = context["ti"].xcom_pull(
        task_ids="query_airflow_metadata_for_failures", key="failure_count"
    ) or 0

    worst = max(by_dag.items(), key=lambda x: x[1]["count"]) if by_dag else ("none", {"count": 0})
    message = (
        f":rotating_light: *High Failure Rate Detected*\n"
        f"*Total failures (24h):* `{total}`\n"
        f"*Worst DAG:* `{worst[0]}` with `{worst[1]['count']}` failures\n"
        f"*All affected DAGs:* `{list(by_dag.keys())}`\n"
        f"*Action:* Check Airflow UI for details and restart failed DAGs"
    )
    for url_env, key in [("SLACK_WEBHOOK_URL", "text"), ("DISCORD_WEBHOOK_URL", "content")]:
        url = os.getenv(url_env, "")
        if url:
            try:
                requests.post(url, json={key: message}, timeout=5)
            except requests.RequestException:
                pass


def _send_ok_summary(**context):
    total = context["ti"].xcom_pull(
        task_ids="query_airflow_metadata_for_failures", key="failure_count"
    ) or 0
    print(f"Daily failure audit complete: {total} failures in 24h (below escalation threshold)")


def _store_failure_summary(**context):
    """
    Writes the failure summary to the ANALYTICS database (not Airflow metadata).
    This separates operational data (Airflow) from business data (analytics).
    The pipeline_failure_audit table is created by weekly_user_cohort_report.py.
    """
    by_dag = context["ti"].xcom_pull(task_ids="analyze_failure_patterns", key="by_dag") or {}
    report_date = context["ds"]

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            for dag_id, info in by_dag.items():
                await conn.execute(
                    """
                    INSERT INTO pipeline_failure_audit
                        (report_date, dag_id, task_id, failure_count, first_failure, last_failure)
                    VALUES ($1::date, $2, $3, $4, NOW() - INTERVAL '24 hours', NOW())
                    ON CONFLICT DO NOTHING
                    """,
                    report_date,
                    dag_id,
                    ", ".join(info.get("tasks", [])),
                    info["count"],
                )
        finally:
            await conn.close()

    asyncio.run(_insert())


with DAG(
    dag_id="failed_task_audit_reporter",
    default_args=default_args,
    schedule_interval="0 7 * * *",   # daily at 07:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["audit", "monitoring", "error-handling"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Query Airflow's OWN metadata DB for failures ──────────────────
    query_airflow_metadata_for_failures = PythonOperator(
        task_id="query_airflow_metadata_for_failures",
        python_callable=_query_airflow_metadata_for_failures,
    )

    analyze_failure_patterns = PythonOperator(
        task_id="analyze_failure_patterns",
        python_callable=_analyze_failure_patterns,
    )

    branch_failure_rate = BranchPythonOperator(
        task_id="branch_failure_rate",
        python_callable=_branch_failure_rate,
    )

    send_escalation_alert = PythonOperator(
        task_id="send_escalation_alert",
        python_callable=_send_escalation_alert,
    )

    send_ok_summary = PythonOperator(
        task_id="send_ok_summary",
        python_callable=_send_ok_summary,
    )

    store_failure_summary = PythonOperator(
        task_id="store_failure_summary",
        python_callable=_store_failure_summary,
        trigger_rule="none_failed_min_one_success",
    )

    (
        query_airflow_metadata_for_failures
        >> analyze_failure_patterns
        >> branch_failure_rate
    )
    branch_failure_rate >> send_escalation_alert >> store_failure_summary
    branch_failure_rate >> send_ok_summary >> store_failure_summary
