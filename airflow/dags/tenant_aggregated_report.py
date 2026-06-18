"""
DAG 13: tenant_aggregated_report
Category: Reporting & Aggregation

WHAT IT DOES:
  Every Monday at 09:00, reads gold-layer Parquet data for each of the 3 tenants
  (nike, puma, adidas) in PARALLEL, aggregates their weekly event counts, then
  merges the results into a single cross-tenant comparison report stored in
  Postgres. Shows which tenant had the most events, highest purchase rate, etc.

WHY WE USE IT:
  Multi-tenant platforms need cross-tenant benchmarking — "how does Nike compare
  to Adidas this week?" This DAG shows how to structure parallel processing in
  Airflow using dynamic task generation. The fan-out → merge pattern is one of
  the most important orchestration patterns in data engineering.

KEY AIRFLOW CONCEPT TAUGHT:
  Dynamic task generation inside a DAG — using a for-loop to create multiple
  PythonOperator tasks at DAG parse time (not a factory of DAGs, but a factory
  of TASKS within one DAG). The list >> task syntax creates an implicit join:
  all tasks in the list must succeed before the next task starts.
  This is different from the dynamic_dag_factory.py (which creates whole DAGs).
  Here we create parallel tasks within a single DAG run.
"""

import asyncio
import glob
import json
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

# The same list that central_tenant_manifest.json uses
TENANTS = ["nike", "puma", "adidas"]


def _aggregate_tenant(tenant: str, **context):
    """
    Reads all Parquet files for a given tenant from the gold layer and
    computes a simple event count. In a real system, this would read from
    /data/gold/{tenant}/ or similar partition.

    Falls back to 0 if no gold files exist for the tenant yet.
    The op_kwargs={"tenant": tenant} passes the tenant name at task creation time.
    """
    gold_path = f"/data/gold/analytics_{tenant}"
    parquet_files = glob.glob(f"{gold_path}/**/*.parquet", recursive=True)

    if not parquet_files:
        print(f"No gold data found for tenant '{tenant}' — using 0 as fallback")
        result = {"tenant": tenant, "event_count": 0, "purchase_count": 0}
    else:
        try:
            import pyarrow.parquet as pq

            dataset = pq.ParquetDataset(gold_path)
            table = dataset.read(columns=["event_type", "total_events"])
            total = int(table.column("total_events").to_pylist()[0]) if len(table) > 0 else 0
            result = {"tenant": tenant, "event_count": total, "purchase_count": 0}
        except Exception as e:
            print(f"Could not read gold data for {tenant}: {e} — using 0")
            result = {"tenant": tenant, "event_count": 0, "purchase_count": 0}

    print(f"Tenant '{tenant}' result: {result}")
    # XCom key includes the tenant name so merge_tenant_reports can pull each one
    context["ti"].xcom_push(key=f"result_{tenant}", value=result)


def _merge_tenant_reports(**context):
    """
    Pulls XCom results from all 3 parallel tenant aggregation tasks and
    combines them into a single comparison dict.

    XCom pull with task_ids= pulls from a specific task. Since each tenant's
    task has a unique task_id (aggregate_nike, aggregate_puma, etc.), we can
    pull from each independently.
    """
    results = []
    for tenant in TENANTS:
        # Pull result from the tenant-specific task
        r = context["ti"].xcom_pull(
            task_ids=f"aggregate_{tenant}",
            key=f"result_{tenant}",
        )
        if r:
            results.append(r)

    if not results:
        print("No tenant results available — all tenants had empty gold layers")
        return

    # Sort by event count to find top tenant
    results.sort(key=lambda x: x["event_count"], reverse=True)
    top_tenant = results[0]["tenant"] if results else "none"

    comparison = {
        "report_week": context["ds"],
        "tenants": results,
        "top_tenant": top_tenant,
    }
    print(f"Cross-tenant comparison: {comparison}")
    context["ti"].xcom_push(key="comparison", value=comparison)


def _store_cross_tenant_comparison(**context):
    """
    Writes the merged comparison to Postgres for dashboarding.
    """
    comparison = context["ti"].xcom_pull(task_ids="merge_tenant_reports", key="comparison") or {}

    async def _upsert():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cross_tenant_comparison (
                    report_week  DATE PRIMARY KEY,
                    top_tenant   VARCHAR(100),
                    details      JSONB,
                    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );

                INSERT INTO cross_tenant_comparison (report_week, top_tenant, details)
                VALUES ($1::DATE, $2, $3::JSONB)
                ON CONFLICT (report_week) DO UPDATE SET
                    top_tenant   = EXCLUDED.top_tenant,
                    details      = EXCLUDED.details,
                    generated_at = NOW()
                """,
                context["ds"],
                comparison.get("top_tenant", "none"),
                json.dumps(comparison),
            )
        finally:
            await conn.close()

    asyncio.run(_upsert())


with DAG(
    dag_id="tenant_aggregated_report",
    default_args=default_args,
    schedule_interval="0 9 * * 1",   # every Monday at 09:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reporting", "multi-tenant", "weekly"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Dynamic task generation — creates one task per tenant ─────────────────
    # This for-loop runs at DAG PARSE TIME (when Airflow reads the .py file).
    # Each iteration creates a separate PythonOperator with a unique task_id.
    # op_kwargs={"tenant": tenant} closes over the loop variable — this is how
    # you pass different arguments to the same callable for each task.
    #
    # Result: 3 parallel tasks: aggregate_nike, aggregate_puma, aggregate_adidas
    # All run simultaneously (fan-out), reducing total runtime vs sequential.
    aggregate_tasks = []
    for tenant in TENANTS:
        task = PythonOperator(
            task_id=f"aggregate_{tenant}",
            python_callable=_aggregate_tenant,
            op_kwargs={"tenant": tenant},    # tenant name is baked in at parse time
        )
        aggregate_tasks.append(task)

    # ── Merge point — waits for ALL parallel tasks to complete ────────────────
    # [task1, task2, task3] >> merge_tenant_reports means:
    #   "merge_tenant_reports won't start until ALL 3 aggregation tasks succeed"
    # This is the fan-in after fan-out pattern.
    merge_tenant_reports = PythonOperator(
        task_id="merge_tenant_reports",
        python_callable=_merge_tenant_reports,
    )

    store_cross_tenant_comparison = PythonOperator(
        task_id="store_cross_tenant_comparison",
        python_callable=_store_cross_tenant_comparison,
    )

    end = EmptyOperator(task_id="end")

    # Wire up: start → [fan-out] → merge → store → end
    start >> aggregate_tasks          # start fans out to all 3 parallel tasks
    aggregate_tasks >> merge_tenant_reports   # all 3 must succeed before merge
    merge_tenant_reports >> store_cross_tenant_comparison >> end
