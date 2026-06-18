"""
DAG 22: conditional_spark_backfill_orchestrator
Category: Advanced Orchestration

WHAT IT DOES:
  Every day at 05:00, scans the /data/gold/ directory for missing date partitions
  in the past 7 days. If any dates are missing (data outage, deployment gap),
  it triggers backfill Spark runs for each missing date, in chronological order
  (oldest first), waiting for each to complete before starting the next.

WHY WE USE IT:
  Outages happen. When the batch ETL is down for 6 hours, 6 hourly Parquet files
  are missing from gold. Without backfill, those gaps are permanent. This DAG
  detects gaps automatically and fills them — the "self-healing pipeline" pattern.

KEY AIRFLOW CONCEPT TAUGHT:
  TriggerDagRunOperator in a loop with wait_for_completion=True:
  - wait_for_completion=True means this task WAITS for the triggered DAG to finish
    before marking itself complete. This is how you create sequential multi-DAG chains.
  - Combined with dynamically generated tasks (for-loop at parse time), this creates
    a sequential backfill pipeline that processes missing dates in order.
  - conf={...} passes the target date to the triggered DAG at runtime.

  Also teaches: scanning filesystem for data gaps, BranchPythonOperator as
  a "nothing to do" guard, and using macros.ds_add() in TriggerDagRunOperator conf.
"""

import glob
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}

LOOKBACK_DAYS = 7   # check the past 7 days for gaps


def _scan_gold_for_missing_dates(**context):
    """
    Checks /data/gold/ for the expected partition structure for each of the
    past LOOKBACK_DAYS. A date is "missing" if no Parquet files exist for it.

    Expected structure (created by batch_etl_pipeline_v1):
    /data/landing/YYYY/MM/DD/HH/events.parquet

    We check landing (not gold) because gold is the Spark aggregation output —
    landing is where the raw hourly files land. Missing landing files = missing data.
    """
    run_date = datetime.strptime(context["ds"], "%Y-%m-%d")
    missing_dates = []

    for days_ago in range(1, LOOKBACK_DAYS + 1):
        check_date = run_date - timedelta(days=days_ago)
        date_str = check_date.strftime("%Y-%m-%d")
        year = check_date.year
        month = check_date.month
        day = check_date.day

        # Check if ANY landing Parquet file exists for this date
        landing_pattern = f"/data/landing/{year:04d}/{month:02d}/{day:02d}/**/*.parquet"
        files = glob.glob(landing_pattern, recursive=True)

        if not files:
            missing_dates.append(date_str)
            print(f"MISSING: No landing data for {date_str}")
        else:
            print(f"OK: Found {len(files)} landing files for {date_str}")

    print(f"Missing dates (will backfill): {missing_dates}")
    context["ti"].xcom_push(key="missing_dates", value=missing_dates)
    return missing_dates


def _branch_backfill_needed(**context):
    missing = context["ti"].xcom_pull(task_ids="scan_gold_for_missing_dates", key="missing_dates") or []
    print(f"Missing dates: {missing}")
    return "prepare_backfill_plan" if missing else "no_backfill_needed"


def _prepare_backfill_plan(**context):
    missing = context["ti"].xcom_pull(task_ids="scan_gold_for_missing_dates", key="missing_dates") or []
    print(f"Backfill plan: will trigger batch_etl_pipeline_v1 for dates: {missing}")
    print("Note: Dates will be triggered in chronological order (oldest first)")
    print("Each trigger waits for completion before starting the next (sequential backfill)")


with DAG(
    dag_id="conditional_spark_backfill_orchestrator",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # only one backfill orchestrator at a time
    tags=["orchestration", "backfill", "advanced"],
    doc_md=__doc__,
) as dag:

    scan_gold_for_missing_dates = PythonOperator(
        task_id="scan_gold_for_missing_dates",
        python_callable=_scan_gold_for_missing_dates,
    )

    branch_backfill_needed = BranchPythonOperator(
        task_id="branch_backfill_needed",
        python_callable=_branch_backfill_needed,
    )

    no_backfill_needed = EmptyOperator(task_id="no_backfill_needed")

    prepare_backfill_plan = PythonOperator(
        task_id="prepare_backfill_plan",
        python_callable=_prepare_backfill_plan,
    )

    # ── Dynamic TriggerDagRunOperator tasks (generated at parse time) ─────────
    #
    # We generate one TriggerDagRunOperator per possible missing day (LOOKBACK_DAYS).
    # At runtime, most of these will be SKIPPED because the branching logic
    # routes to prepare_backfill_plan only when missing_dates is non-empty.
    # The sequential chaining (prev >> trigger >> next) ensures:
    #   1. Day 7 backfill runs first (oldest missing date)
    #   2. Day 6 backfill starts only after Day 7 completes
    #   3. ... and so on
    # wait_for_completion=True means the TriggerDagRunOperator HOLDS until
    # the triggered batch_etl_pipeline_v1 run finishes (success or failure).
    #
    # conf passes the backfill date — {{ macros.ds_add(ds, -(LOOKBACK_DAYS - i)) }}
    # is a Jinja template that subtracts N days from today's date.
    trigger_tasks = []
    prev_task = prepare_backfill_plan

    for day_offset in range(LOOKBACK_DAYS, 0, -1):  # 7, 6, 5, 4, 3, 2, 1 (oldest first)
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_backfill_day_minus_{day_offset}",
            trigger_dag_id="batch_etl_pipeline_v1",
            wait_for_completion=True,   # wait for triggered DAG to finish
            reset_dag_run=True,         # reset if a run for this date already exists
            conf={
                "backfill_date": "{{ macros.ds_add(ds, -" + str(day_offset) + ") }}",
                "triggered_by": "conditional_spark_backfill_orchestrator",
            },
        )
        prev_task >> trigger_task
        trigger_tasks.append(trigger_task)
        prev_task = trigger_task

    backfill_complete = EmptyOperator(
        task_id="backfill_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # Wire: scan → branch → [no_backfill | prepare → trigger_chain → complete]
    scan_gold_for_missing_dates >> branch_backfill_needed
    branch_backfill_needed >> no_backfill_needed >> backfill_complete
    branch_backfill_needed >> prepare_backfill_plan  # prepare leads into trigger chain
    if trigger_tasks:
        trigger_tasks[-1] >> backfill_complete
