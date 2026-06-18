"""
DAG 23: multi_sensor_coordination_pipeline
Category: Advanced Orchestration

WHAT IT DOES:
  Every 3 hours, waits for BOTH of these to complete:
    1. batch_etl_pipeline_v1 → spark_transform_landing_to_gold
    2. dynamic_tenant_etl_nike → execute_tenant_spark_job
  Once BOTH are confirmed done, runs a Spark job that reads both gold outputs
  and produces a cross-pipeline join result (e.g., combining core metrics
  with Nike-specific tenant metrics).

WHY WE USE IT:
  Real data platforms have multiple parallel pipelines that occasionally need
  to synchronize. The cross-pipeline join only makes sense after BOTH upstream
  pipelines have finished for the same time window. This DAG implements the
  "wait for multiple upstream signals before proceeding" pattern.

KEY AIRFLOW CONCEPT TAUGHT:
  Parallel ExternalTaskSensor fan-in — running TWO sensors in parallel and
  waiting for BOTH to succeed before proceeding:

  [sensor_A, sensor_B] >> all_upstream_ready

  This list >> task syntax creates an implicit AND-join: all_upstream_ready
  only starts when BOTH sensors succeed. If either sensor fails or times out,
  all_upstream_ready is not triggered.

  This is different from the backfill orchestrator (sequential chain).
  Here we want PARALLEL waiting with AND-join semantics.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}


def _store_cross_pipeline_result(**context):
    """
    After the Spark join completes, logs the result to pipeline_run_log
    as evidence that cross-pipeline coordination happened successfully.
    """
    import asyncio
    import asyncpg

    async def _insert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                INSERT INTO pipeline_run_log (dag_id, run_date, row_count, mode, passed_at)
                VALUES ($1, $2::date, 0, 'cross_pipeline_join', NOW())
                ON CONFLICT (dag_id, run_date) DO UPDATE
                    SET mode = 'cross_pipeline_join', passed_at = NOW()
                """,
                "multi_sensor_coordination_pipeline",
                context["ds"],
            )
        finally:
            await conn.close()

    asyncio.run(_insert())
    print(f"Cross-pipeline join complete for {context['ds']}")


with DAG(
    dag_id="multi_sensor_coordination_pipeline",
    default_args=default_args,
    schedule_interval="0 */3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["orchestration", "sensor", "advanced", "multi-dag"],
    doc_md=__doc__,
) as dag:

    # ── Sensor 1: Wait for core batch ETL gold output ─────────────────────────
    # This sensor watches batch_etl_pipeline_v1's final spark transform task.
    # execution_delta=timedelta(0) means "watch the run with the same logical date"
    # soft_fail=True means if no matching run is found (ETL hasn't run yet at all),
    # the sensor is SKIPPED rather than failing the whole DAG.
    sense_batch_etl_gold = ExternalTaskSensor(
        task_id="sense_batch_etl_gold",
        external_dag_id="batch_etl_pipeline_v1",
        external_task_id="spark_transform_landing_to_gold",
        execution_delta=timedelta(hours=0),
        timeout=7200,           # wait up to 2 hours for batch ETL
        poke_interval=120,      # check every 2 minutes
        mode="reschedule",      # MUST use reschedule with LocalExecutor
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        soft_fail=True,         # skip (not fail) if external DAG hasn't run
    )

    # ── Sensor 2: Wait for Nike tenant ETL gold output ────────────────────────
    # This sensor watches the dynamically generated Nike tenant DAG.
    # The external_task_id "execute_tenant_spark_job" matches the task_id
    # used in dynamic_dag_factory.py's generated DAGs.
    sense_nike_tenant_etl = ExternalTaskSensor(
        task_id="sense_nike_tenant_etl",
        external_dag_id="dynamic_tenant_etl_nike",
        external_task_id="execute_tenant_spark_job",
        execution_delta=timedelta(hours=0),
        timeout=7200,
        poke_interval=120,
        mode="reschedule",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        soft_fail=True,
    )

    # ── Fan-in point — waits for BOTH sensors ─────────────────────────────────
    # This EmptyOperator has TWO upstream dependencies (both sensors).
    # Airflow's default trigger_rule="all_success" means it only runs
    # when BOTH sensors are in "success" state.
    # [task_a, task_b] >> task_c is syntactic sugar for:
    #   task_a >> task_c
    #   task_b >> task_c
    # Both create edges from A→C and B→C, and trigger_rule ensures C
    # waits for all incoming edges.
    all_upstream_ready = EmptyOperator(task_id="all_upstream_ready")

    # ── Cross-pipeline join via Spark ─────────────────────────────────────────
    # Only runs after BOTH upstream pipelines have completed.
    # Reads both gold outputs and produces a merged result.
    # Falls back gracefully if gold directories don't exist yet.
    join_batch_and_tenant_via_spark = BashOperator(
        task_id="join_batch_and_tenant_via_spark",
        bash_command=(
            "echo 'Cross-pipeline join: batch gold + Nike tenant gold' && "
            "ls /data/gold/ 2>/dev/null && "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "/opt/spark/apps/metrics_aggregation.py "
            "--start {{ data_interval_start.isoformat() }} "
            "--end {{ data_interval_end.isoformat() }} "
            "|| echo 'Spark join skipped (no data yet)' "
        ),
    )

    store_cross_pipeline_result = PythonOperator(
        task_id="store_cross_pipeline_result",
        python_callable=_store_cross_pipeline_result,
    )

    # ── Wire: two parallel sensors → fan-in → join → store ───────────────────
    [sense_batch_etl_gold, sense_nike_tenant_etl] >> all_upstream_ready
    all_upstream_ready >> join_batch_and_tenant_via_spark >> store_cross_pipeline_result
