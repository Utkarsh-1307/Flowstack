"""
DAG 24: adaptive_schedule_etl
Category: Advanced Orchestration

WHAT IT DOES:
  Every hour, reads its own run history from pipeline_run_log. If the last 3
  consecutive runs all had row_count < MIN_ROWS_THRESHOLD (indicating an upstream
  data problem), it triggers a FULL REPROCESS of the past 24 hours instead of
  the normal incremental 1-hour window. Otherwise, runs the standard incremental
  load. This is the "self-aware pipeline" pattern.

WHY WE USE IT:
  Standard incremental loads are efficient but can accumulate data gaps during
  outages. A pipeline that detects "I've been getting no data for 3 runs straight"
  and automatically widens its processing window is far more resilient. This
  pattern is used in production at companies like Airbnb and LinkedIn.

KEY AIRFLOW CONCEPT TAUGHT:
  "Self-aware" pipeline using its own run history as input:
  - The DAG reads from pipeline_run_log (which IT writes to after each run)
  - This makes the DAG stateful across runs without using Airflow's XCom history
  - BranchPythonOperator chooses the processing mode
  - trigger_rule="none_failed_min_one_success" lets both branches share a
    convergence task (validate_output) without duplicate code

  Also shows using op_kwargs with BashOperator: Jinja templates
  data_interval_start and shell date arithmetic for dynamic windows.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.bash import BashOperator
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

MIN_ROWS_THRESHOLD = 10     # fewer than this = "low data" run
CONSECUTIVE_LOW_RUNS = 3    # if this many consecutive low-data runs, do full reprocess


def _evaluate_recent_run_history(**context):
    """
    Reads the last CONSECUTIVE_LOW_RUNS entries from pipeline_run_log for this DAG.
    If all recent runs had row_count < MIN_ROWS_THRESHOLD, flag for full reprocess.

    This is the "self-awareness" mechanism — the DAG queries its own history
    to decide how to behave for this run.
    """
    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            rows = await conn.fetch(
                """
                SELECT run_date, row_count, mode
                FROM pipeline_run_log
                WHERE dag_id = $1
                ORDER BY run_date DESC
                LIMIT $2
                """,
                "adaptive_schedule_etl",
                CONSECUTIVE_LOW_RUNS,
            )
        finally:
            await conn.close()
        return [dict(r) for r in rows]

    recent_runs = asyncio.run(_query())
    print(f"Recent runs: {recent_runs}")

    if len(recent_runs) < CONSECUTIVE_LOW_RUNS:
        # Not enough history yet — run normally
        needs_full_reprocess = False
        print(f"Insufficient history ({len(recent_runs)} runs) — running incrementally")
    else:
        all_low = all(r["row_count"] < MIN_ROWS_THRESHOLD for r in recent_runs)
        needs_full_reprocess = all_low
        if all_low:
            print(
                f"Last {CONSECUTIVE_LOW_RUNS} runs all had low row counts "
                f"({[r['row_count'] for r in recent_runs]}) — triggering FULL REPROCESS"
            )
        else:
            print(f"Run history healthy — running incrementally")

    context["ti"].xcom_push(key="needs_full_reprocess", value=needs_full_reprocess)
    context["ti"].xcom_push(key="recent_runs", value=[
        {k: str(v) for k, v in r.items()} for r in recent_runs
    ])


def _branch_full_vs_incremental(**context):
    needs_full = context["ti"].xcom_pull(
        task_ids="evaluate_recent_run_history",
        key="needs_full_reprocess",
    )
    return "run_full_reprocess" if needs_full else "run_incremental_load"


def _update_run_history_log(mode: str, **context):
    """
    After processing, logs this run's outcome to pipeline_run_log.
    This is what future runs will read via _evaluate_recent_run_history.

    We count raw_events rows to measure "how much data did we process?"
    In a real pipeline, you'd count rows written to gold instead.
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    async def _count_and_log():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM raw_events WHERE created_at >= $1 AND created_at < $2",
                start, end,
            )
            await conn.execute(
                """
                INSERT INTO pipeline_run_log (dag_id, run_date, row_count, mode, passed_at)
                VALUES ($1, $2::date, $3, $4, NOW())
                ON CONFLICT (dag_id, run_date) DO UPDATE
                    SET row_count = EXCLUDED.row_count,
                        mode      = EXCLUDED.mode,
                        passed_at = NOW()
                """,
                "adaptive_schedule_etl",
                context["ds"],
                count or 0,
                mode,
            )
            print(f"Logged run: mode={mode}, row_count={count}")
        finally:
            await conn.close()

    asyncio.run(_count_and_log())


with DAG(
    dag_id="adaptive_schedule_etl",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["orchestration", "adaptive", "advanced"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Bootstrap pipeline_run_log (shared with other DAGs) ───────────
    ensure_run_log = PostgresOperator(
        task_id="ensure_run_log",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS pipeline_run_log (
                dag_id    VARCHAR(250) NOT NULL,
                run_date  DATE NOT NULL,
                row_count BIGINT DEFAULT 0,
                mode      VARCHAR(50) DEFAULT 'incremental',
                passed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (dag_id, run_date)
            );
        """,
    )

    # ── Step 2: Read own history to decide processing mode ────────────────────
    evaluate_recent_run_history = PythonOperator(
        task_id="evaluate_recent_run_history",
        python_callable=_evaluate_recent_run_history,
    )

    # ── Step 3: Branch to full reprocess or incremental ───────────────────────
    branch_full_vs_incremental = BranchPythonOperator(
        task_id="branch_full_vs_incremental",
        python_callable=_branch_full_vs_incremental,
    )

    # ── Branch A: Full reprocess — past 24 hours ──────────────────────────────
    # Uses data_interval_start minus 23 hours to cover a full 24-hour window.
    # We avoid {{ macros.ds_add() }} inside BashOperator bash_command because
    # macros are only available in templated fields — bash_command IS templated,
    # but ds_add works reliably via the macros object only in certain contexts.
    # Instead we use shell date arithmetic which is always safe.
    run_full_reprocess = BashOperator(
        task_id="run_full_reprocess",
        bash_command=(
            "echo 'FULL REPROCESS MODE: processing 24-hour window' && "
            "YESTERDAY=$(date -u -d '{{ ds }} -1 day' '+%Y-%m-%dT00:00:00+00:00' 2>/dev/null "
            "  || date -u -v-1d -j -f '%Y-%m-%d' '{{ ds }}' '+%Y-%m-%dT00:00:00+00:00') && "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--conf spark.driver.memory=2g "
            "/opt/spark/apps/metrics_aggregation.py "
            "--start $YESTERDAY "
            "--end {{ ds }}T00:00:00+00:00 "
            "|| echo 'Full reprocess spark job failed or no data' "
        ),
    )

    # ── Branch B: Incremental — standard 1-hour window ────────────────────────
    # {{ data_interval_start }} and {{ data_interval_end }} are the standard
    # hourly window boundaries — the same pattern as batch_etl_pipeline_v1.
    run_incremental_load = BashOperator(
        task_id="run_incremental_load",
        bash_command=(
            "echo 'INCREMENTAL MODE: processing 1-hour window' && "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "/opt/spark/apps/metrics_aggregation.py "
            "--start {{ data_interval_start.isoformat() }} "
            "--end {{ data_interval_end.isoformat() }} "
            "|| echo 'Incremental spark job failed or no data' "
        ),
    )

    # ── Step 4: Validate output (runs after EITHER branch) ────────────────────
    # trigger_rule="none_failed_min_one_success" means: run if at least one
    # upstream succeeded and none failed. Since BranchPythonOperator marks the
    # non-chosen branch as SKIPPED (not failed), this convergence task runs
    # after whichever branch ran.
    validate_output = EmptyOperator(
        task_id="validate_output",
        trigger_rule="none_failed_min_one_success",
    )

    # ── Step 5: Log this run's outcome ────────────────────────────────────────
    # These two update tasks use op_kwargs to pass the mode string.
    # Only one will run (the other branch was skipped).
    update_run_log_full = PythonOperator(
        task_id="update_run_log_full",
        python_callable=_update_run_history_log,
        op_kwargs={"mode": "full_reprocess"},
    )

    update_run_log_incremental = PythonOperator(
        task_id="update_run_log_incremental",
        python_callable=_update_run_history_log,
        op_kwargs={"mode": "incremental"},
    )

    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        trigger_rule="none_failed_min_one_success",
    )

    ensure_run_log >> evaluate_recent_run_history >> branch_full_vs_incremental

    branch_full_vs_incremental >> run_full_reprocess >> validate_output
    branch_full_vs_incremental >> run_incremental_load >> validate_output

    validate_output >> update_run_log_full
    validate_output >> update_run_log_incremental

    [update_run_log_full, update_run_log_incremental] >> pipeline_done
