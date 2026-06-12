"""
Dynamic DAG Factory — reads central_tenant_manifest.json and injects one DAG
per tenant into Airflow's global namespace. Adding a new tenant requires only
a JSON entry; no Python changes needed.
"""
import json
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../config/central_tenant_manifest.json")

with open(CONFIG_PATH, "r") as f:
    tenants = json.load(f)


def _build_dag(tenant: dict) -> DAG:
    tenant_id = tenant["client_id"]
    dag = DAG(
        dag_id=f"dynamic_tenant_etl_{tenant_id}",
        schedule_interval=tenant["schedule"],
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data_engineering",
            "retries": 2,
            "on_failure_callback": on_failure_alert,
        },
        tags=["multi-tenant", tenant_id],
    )
    with dag:
        BashOperator(
            task_id="execute_tenant_spark_job",
            bash_command=(
                f"spark-submit --master spark://spark-master:7077 "
                f"/opt/spark/apps/tenant_job.py "
                f"--tenant {tenant_id} "
                f"--target-table {tenant['target_table']} "
                f"--retention-days {tenant['retention_days']}"
            ),
        )
    return dag


for _tenant in tenants:
    _dag_id = f"dynamic_tenant_etl_{_tenant['client_id']}"
    globals()[_dag_id] = _build_dag(_tenant)
