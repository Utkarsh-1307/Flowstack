from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "depends_on_past": True,  # each run must succeed before the next window starts
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}


def run_extraction_worker(data_interval_start, data_interval_end, **context):
    """
    Pulls raw_events from Postgres for the closed time window
    [data_interval_start, data_interval_end) and writes Parquet to the landing zone.
    Using explicit window bounds makes this idempotent — re-running the same
    interval always produces the same output with no duplicate rows.
    """
    import asyncio
    import asyncpg
    import pyarrow as pa
    import pyarrow.parquet as pq
    import os

    start_ts = data_interval_start.isoformat()
    end_ts = data_interval_end.isoformat()

    async def extract():
        conn = await asyncpg.connect(os.environ["DATABASE_URL"].replace("+asyncpg", ""))
        rows = await conn.fetch(
            """
            SELECT id, user_id::text, event_type, payload, created_at
            FROM raw_events
            WHERE created_at >= $1 AND created_at < $2
            ORDER BY created_at
            """,
            data_interval_start,
            data_interval_end,
        )
        await conn.close()
        return rows

    rows = asyncio.run(extract())
    if not rows:
        print(f"No events in window {start_ts} → {end_ts}. Skipping write.")
        return

    table = pa.table({
        "id": [r["id"] for r in rows],
        "user_id": [r["user_id"] for r in rows],
        "event_type": [r["event_type"] for r in rows],
        "payload": [str(r["payload"]) for r in rows],
        "created_at": [r["created_at"] for r in rows],
    })

    year = data_interval_start.year
    month = data_interval_start.month
    day = data_interval_start.day
    hour = data_interval_start.hour
    out_path = f"/data/landing/{year:04d}/{month:02d}/{day:02d}/{hour:02d}"
    os.makedirs(out_path, exist_ok=True)
    pq.write_table(table, f"{out_path}/events.parquet")
    print(f"Wrote {len(rows)} rows to {out_path}/events.parquet")


with DAG(
    dag_id="batch_etl_pipeline_v1",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "batch"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_postgres_to_landing",
        python_callable=run_extraction_worker,
    )

    spark_transform_task = BashOperator(
        task_id="spark_transform_landing_to_gold",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "--deploy-mode client "
            "/opt/spark/apps/metrics_aggregation.py "
            "--start {{ data_interval_start.isoformat() }} "
            "--end {{ data_interval_end.isoformat() }}"
        ),
    )

    extract_task >> spark_transform_task
