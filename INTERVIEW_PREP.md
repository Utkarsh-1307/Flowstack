# FlowStack — Interview Preparation Guide

> Complete line-by-line explanation of every concept, pattern, and design decision.
> Read this top to bottom once, then use the headings to revise specific topics.

---

## Table of Contents

1. [The Big Picture](#1-the-big-picture)
2. [Docker & Infrastructure](#2-docker--infrastructure)
3. [Airflow Core Concepts](#3-airflow-core-concepts)
4. [Core ETL DAG](#4-core-etl-dag)
5. [Data Quality DAGs](#5-data-quality-dags)
6. [Reporting DAGs](#6-reporting-dags)
7. [Advanced Orchestration DAGs](#7-advanced-orchestration-dags)
8. [Data Lifecycle DAGs](#8-data-lifecycle-dags)
9. [Monitoring DAGs](#9-monitoring-dags)
10. [Dynamic DAG Factory](#10-dynamic-dag-factory)
11. [Spark Jobs](#11-spark-jobs)
12. [Key Interview Q&A](#12-key-interview-qa)

---

## 1. The Big Picture

### What to say first in any interview

> "FlowStack is a production-grade data engineering platform with two data paths:
>
> **Streaming path** — Kafka receives events, Spark Structured Streaming reads them in micro-batches and writes aggregated metrics to PostgreSQL in near real-time.
>
> **Batch path** — Apache Airflow schedules hourly Spark jobs that read from a Parquet landing zone, aggregate the data, and write results to a gold layer in the data lake.
>
> The entire orchestration layer is 26 Airflow DAGs covering ETL, data quality, reporting, monitoring, and advanced orchestration patterns like backfill, branch convergence, and self-aware pipelines."

### The Lakehouse Pattern (Landing → Bronze → Gold)

| Layer | Path | What it contains | Rule |
|---|---|---|---|
| Landing | `/data/landing/YYYY/MM/DD/HH/` | Raw Parquet dumps, one file per hour | Immutable — never modified |
| Bronze | `/data/bronze/` | Validated, cleaned records | Reprocessed from landing if corrupted |
| Gold | `/data/gold/` | Aggregated, query-optimised output | Reprocessed from bronze if corrupted |

**Why three layers?**
Each layer is a checkpoint. If gold is corrupted, reprocess from bronze. If bronze is corrupted, reprocess from landing. The raw source is never touched — you always have a recovery path.

---

## 2. Docker & Infrastructure

### How Docker Compose works

Docker Compose reads `docker-compose.yml` and starts all services as containers on a shared virtual network (`data_platform_network`). Containers address each other by **service name** — e.g. `kafka-broker:29092`, `postgres-db:5432`.

---

### PostgreSQL service

```yaml
postgres-db:
  image: postgres:15-alpine
  # alpine = stripped-down Linux image, smaller download and attack surface
  container_name: production_postgres
  environment:
    POSTGRES_DB: analytics_platform    # database name
    POSTGRES_USER: engine_admin        # superuser username
    POSTGRES_PASSWORD: ${DATABASE_SECURE_PASSWORD}  # read from .env file
  volumes:
    - pg_data_store:/var/lib/postgresql/data
    # Named volume: data survives "docker compose down" and "docker compose up"
    # Without this, all data is lost when the container stops
    - ./docker/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    # Postgres auto-runs any .sql file in this directory on FIRST startup
    # This creates all our tables, indexes, and initial schema
  ports:
    - "5433:5432"
    # host_port:container_port
    # Access from your laptop: localhost:5433
    # Access from other containers: postgres-db:5432
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U engine_admin -d analytics_platform"]
    # pg_isready = Postgres built-in tool, returns exit code 0 if DB accepts connections
    interval: 10s    # run this check every 10 seconds
    timeout: 5s      # if no response in 5s, count as failed
    retries: 5       # mark unhealthy after 5 consecutive failures
```

**Interview answer for healthcheck:**
> "The healthcheck lets other services use `condition: service_healthy` in their `depends_on`. This means Airflow won't start until Postgres is actually accepting connections — not just when the container process started. Without this, Airflow's `db migrate` command would fail because Postgres isn't ready yet."

---

### Kafka service (KRaft mode)

```yaml
kafka-broker:
  image: confluentinc/cp-kafka:7.6.1
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: 'broker,controller'
    # KRaft mode: same node acts as both the broker (handles messages)
    # AND the controller (manages cluster metadata, leader elections)
    # Old mode used ZooKeeper as a separate process for the controller role

    KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka-broker:29093'
    # Raft quorum: node ID 1 at kafka-broker:29093 has a vote
    # In a 3-node cluster this would be '1@host1:29093,2@host2:29093,3@host3:29093'

    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
    # Three named listeners, all using PLAINTEXT (no TLS — dev environment)
    # CONTROLLER: internal controller communication
    # PLAINTEXT: internal Docker network (other containers)
    # PLAINTEXT_HOST: external access from your laptop

    KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka-broker:29092,PLAINTEXT_HOST://localhost:9092'
    # "Advertised" = what Kafka tells clients to connect to when they ask "where is the broker?"
    # Inside Docker: use kafka-broker:29092
    # Outside Docker (your laptop): use localhost:9092
```

**Why KRaft over ZooKeeper?**
ZooKeeper mode requires running two separate processes — Kafka broker + ZooKeeper. KRaft uses Raft consensus built into Kafka itself. One process, simpler ops, and it's where the Kafka project is heading for all future versions.

---

### Spark Cluster

```yaml
spark-master:
  ports:
    - "8082:8080"   # Spark Web UI — shows workers, running jobs, memory usage
    - "7077:7077"   # Spark Master port — workers register here, drivers submit jobs here
  volumes:
    - ./data:/data              # /data on your laptop = /data inside container
    - ./spark/apps:/opt/spark/apps  # PySpark scripts available inside container

spark-worker-1:
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077   # register with master at startup
    - SPARK_WORKER_MEMORY=2G   # memory available to tasks on this worker
    - SPARK_WORKER_CORES=2     # CPU cores available on this worker
```

**Interview answer:**
> "All three Spark containers mount the same `./data` host directory. This simulates a shared filesystem — the master and both workers all read/write the same `/data/` path. In production, this would be S3 or HDFS; the PySpark code stays identical, only the path prefix changes (`s3://bucket/` instead of `/data/`)."

---

### Airflow services

```yaml
airflow-init:
  # Runs ONCE at startup to set up the database and admin user
  command:
    - -c
    - |
      airflow db migrate &&
      # "db migrate" creates or upgrades all of Airflow's metadata tables in Postgres
      # Tables: dag, dag_run, task_instance, xcom, connection, variable, log, etc.
      airflow users create \
        --username admin \
        --role Admin \
        --password admin
  depends_on:
    postgres-db:
      condition: service_healthy
      # Uses the healthcheck — won't start until Postgres is actually ready

airflow-webserver:
  command: webserver
  ports:
    - "8080:8080"   # Airflow UI
  volumes:
    - ./airflow/dags:/opt/airflow/dags          # DAG files live here — hot-reloaded
    - ./airflow/plugins:/opt/airflow/plugins    # Custom operators, hooks, callbacks
    - ./data:/data                               # Same data lake
  depends_on:
    airflow-init:
      condition: service_completed_successfully
      # Wait for init container to finish (exit 0) before starting

airflow-scheduler:
  command: scheduler
  # The scheduler reads DAG files, decides what to run and when,
  # and hands tasks to the executor (LocalExecutor = subprocess on same machine)
```

---

## 3. Airflow Core Concepts

### What Airflow is

Airflow is a **workflow orchestrator**. You write pipelines as Python code. The scheduler reads your Python files, parses the DAG objects, and runs tasks at the right time in the right order.

**Key objects:**

| Object | What it is |
|---|---|
| `DAG` | The pipeline definition — schedule, start date, task graph |
| `Operator` | A single unit of work — PythonOperator, BashOperator, PostgresOperator |
| `Task Instance` | One execution of one operator in one specific DAG run |
| `DAG Run` | One execution of the full DAG for a given logical date |
| `XCom` | Cross-task communication — small values stored in Postgres |
| `Connection` | Stored credentials for external systems (DB, S3, Slack) |
| `Variable` | Runtime configuration values stored in Airflow's DB |

---

### default_args

```python
default_args = {
    "owner": "data_engineering",
    # Shown in Airflow UI. Used to filter DAGs by team.

    "retries": 2,
    # If a task fails, retry it 2 more times before marking it FAILED

    "retry_delay": timedelta(minutes=5),
    # Wait 5 minutes between retry attempts
    # Gives transient issues (network blip, DB restart) time to resolve

    "on_failure_callback": on_failure_alert,
    # Function to call when a task fails after ALL retries
    # Receives a context dict with full info about the failure
}
```

These are **defaults** applied to every task in the DAG. You can override any key per-task.

---

### The alerts plugin

**File:** `airflow/plugins/alerts.py`

```python
def on_failure_alert(context):
    # context is a dict Airflow passes with everything about the failed run
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    # log_url = direct link to the task's log in the Airflow UI

    message = f"DAG `{dag_id}` task `{task_id}` FAILED\nLogs: {log_url}"

    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_url:
        requests.post(slack_url, json={"text": message})
    # Slack Incoming Webhooks: POST a JSON payload, message appears in the channel
    # No Slack SDK needed — plain HTTP POST

    discord_url = os.getenv("DISCORD_WEBHOOK_URL")
    if discord_url:
        requests.post(discord_url, json={"content": message})
```

**Interview answer:**
> "Every DAG sets `on_failure_callback: on_failure_alert` in `default_args`. When any task fails after all retries, Airflow calls this function with a context dict. We POST to Slack and Discord webhooks — no SDK, just `requests.post()`. The webhook URLs come from environment variables so they're never in the code."

---

### Jinja Templating in Airflow

Airflow uses Jinja2 to render dynamic values into **templated fields** at task execution time.

**Built-in variables available in every task:**

| Variable | Type | Example value |
|---|---|---|
| `{{ ds }}` | string | `2026-06-18` |
| `{{ yesterday_ds }}` | string | `2026-06-17` |
| `{{ next_ds }}` | string | `2026-06-19` |
| `{{ data_interval_start }}` | datetime | `2026-06-18 14:00:00+00:00` |
| `{{ data_interval_end }}` | datetime | `2026-06-18 15:00:00+00:00` |
| `{{ macros.ds_add(ds, -7) }}` | string | `2026-06-11` |

**Which fields are templated?**
Not all fields on all operators. Each operator declares which fields are templated. For example:
- `BashOperator.bash_command` — YES, templated
- `PostgresOperator.sql` — YES, templated
- `PythonOperator.python_callable` — NO (use `context["ds"]` inside the function instead)

**Parse-time vs runtime:** Jinja is rendered at **task execution time**, not when the DAG file is loaded. This is why `{{ macros }}` and `{{ var }}` inside docstrings cause parse errors — docstrings are rendered when Airflow loads the file, before the runtime context exists.

---

### XCom (Cross-task Communication)

```python
# Task A: push a value
def task_a(**context):
    result = {"null_pct": 3.7, "total_rows": 10000}
    context["ti"].xcom_push(key="stats", value=result)
    # ti = TaskInstance object
    # Stored in Airflow's Postgres DB: dag_id + run_id + task_id + key → value

# Task B: pull the value
def task_b(**context):
    stats = context["ti"].xcom_pull(task_ids="task_a", key="stats")
    # task_ids = which task pushed it
    # key = the key used in xcom_push
    print(stats["null_pct"])   # 3.7
```

**XCom limits:**
- Stored in Postgres — practical limit ~48KB per value
- For large data: write to `/data/` or Postgres, pass the file path via XCom
- Cleared automatically after `xcom_cleanup_dag` runs (configurable retention)

---

### The `**context` argument

Every PythonOperator callable receives `**context` when `provide_context=True` (default in Airflow 2.x). Key items:

```python
def my_function(**context):
    context["ti"]                    # TaskInstance object
    context["dag"]                   # DAG object
    context["ds"]                    # logical date as string "YYYY-MM-DD"
    context["data_interval_start"]   # window start as datetime
    context["data_interval_end"]     # window end as datetime
    context["run_id"]                # unique run identifier
```

---

## 4. Core ETL DAG

**File:** `airflow/dags/batch_etl_pipeline.py`

### DAG definition

```python
with DAG(
    dag_id="batch_etl_pipeline_v1",
    schedule_interval="@hourly",
    # Cron equivalent: "0 * * * *" — runs at minute 0 of every hour
    # Other presets: @daily, @weekly, @monthly, @once

    start_date=datetime(2026, 1, 1),
    # Airflow will not schedule runs before this date

    catchup=False,
    # If you deploy this DAG today but start_date was 6 months ago,
    # catchup=False means "don't run all the missed hours"
    # catchup=True would queue hundreds of past runs

    max_active_runs=1,
    # Only one DAG run executes at a time
    # If the hourly job takes 90 min, prevents two overlapping runs
    # writing to the same Parquet partition simultaneously
) as dag:
```

---

### Idempotent extraction

```python
def _extract_events(**context):
    start = context["data_interval_start"]   # e.g. 2026-06-18 14:00:00+00
    end   = context["data_interval_end"]     # e.g. 2026-06-18 15:00:00+00

    # Query ONLY events in this exact 1-hour window
    # Using half-open interval [start, end) — start inclusive, end exclusive
    # This means no event is ever counted in two different windows
    rows = db.query(
        "SELECT * FROM raw_events WHERE created_at >= %s AND created_at < %s",
        start, end
    )

    # Partitioned write path: /data/landing/2026/06/18/14/events.parquet
    path = f"/data/landing/{start.year}/{start.month:02d}/{start.day:02d}/{start.hour:02d}/"
    os.makedirs(path, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path + "events.parquet", index=False)
    # Writing the same file twice (retry) = overwrite = same result
    # This is idempotency: run it 10 times, get the same output
```

**Idempotency definition for interview:**
> "Idempotency means running a task multiple times produces the same result. We achieve it three ways: (1) query a fixed time window using `data_interval_start/end` so retries read the same rows, (2) write to a fixed Parquet path so retries overwrite the same file, (3) use `ON CONFLICT DO UPDATE` in SQL inserts so retries update rather than duplicate."

---

### Spark submit via BashOperator

```python
spark_transform = BashOperator(
    task_id="spark_transform_landing_to_gold",
    bash_command=(
        "spark-submit "
        "--master spark://spark-master:7077 "
        # Connect to Spark master at this address
        # spark:// is Spark's standalone cluster protocol

        "--deploy-mode client "
        # client = driver process runs in THIS container (the Airflow worker)
        # cluster = driver runs on a Spark worker node
        # client is simpler for debugging; cluster is used in production for reliability

        "--conf spark.driver.memory=2g "
        # Allocate 2GB RAM to the driver process

        "/opt/spark/apps/metrics_aggregation.py "
        # Path to the PySpark script inside the container

        "--start {{ data_interval_start.isoformat() }} "
        "--end {{ data_interval_end.isoformat() }}"
        # Jinja templates — rendered at runtime to actual timestamps
        # .isoformat() = "2026-06-18T14:00:00+00:00" format
        # Passed as command-line arguments to the PySpark script
    ),
)
```

---

### Task dependency syntax

```python
extract_to_landing >> spark_transform >> validate_gold_output
# >> is the "set_downstream" operator
# This creates edges: extract → spark → validate
# Equivalent to:
# extract_to_landing.set_downstream(spark_transform)
# spark_transform.set_downstream(validate_gold_output)

# Parallel tasks (fan-out):
start >> [task_a, task_b, task_c]
# start runs first, then task_a, task_b, task_c all run in parallel

# Fan-in (AND-join):
[task_a, task_b, task_c] >> end
# end waits for ALL three to succeed before running
```

---

## 5. Data Quality DAGs

### BranchPythonOperator

**File:** `airflow/dags/dq_null_check_pipeline.py`

```python
def _branch_null_check(**context):
    # Pull the null percentage computed by the previous task
    null_pct = context["ti"].xcom_pull(
        task_ids="compute_null_stats",
        key="null_pct"
    )

    if null_pct >= 5.0:                # threshold: 5% nulls = problem
        return "quarantine_records"    # return the task_id to run next
    return "pass_gate"
    # BranchPythonOperator:
    # - Returns a task_id string (or list of strings for multiple branches)
    # - The chosen branch runs normally
    # - ALL other branches are marked SKIPPED (not FAILED)
    # - SKIPPED ≠ FAILED — downstream of skipped tasks also get skipped

branch = BranchPythonOperator(
    task_id="branch_null_check",
    python_callable=_branch_null_check,
)
```

**Branch convergence — the critical pattern:**

```python
dq_complete = EmptyOperator(
    task_id="dq_complete",
    trigger_rule="none_failed_min_one_success",
    # WHY NOT default "all_success"?
    # Default trigger_rule="all_success" requires ALL upstream tasks to succeed
    # But one branch is always SKIPPED — "all_success" treats SKIPPED as not-success
    # So "all_success" would never trigger dq_complete
    #
    # "none_failed_min_one_success":
    # - "none_failed" = no upstream task is in FAILED state
    # - "min_one_success" = at least one upstream task succeeded
    # This fires when one branch succeeded and the other was skipped — exactly what we want
)

branch >> quarantine_records >> dq_complete
branch >> pass_gate          >> dq_complete
```

**Interview answer:**
> "BranchPythonOperator returns a task_id string. The chosen branch runs; all others are SKIPPED. The convergence EmptyOperator needs `trigger_rule='none_failed_min_one_success'` — the default `all_success` would never fire because one branch is always skipped."

---

### ShortCircuitOperator

**File:** `airflow/dags/dq_schema_drift_detector.py`

```python
def _check_landing_has_files(**context):
    files = glob.glob("/data/landing/**/*.parquet", recursive=True)
    # glob.glob with recursive=True searches all subdirectories
    return len(files) > 0
    # True  = continue, run all downstream tasks normally
    # False = skip ALL downstream tasks (they get SKIPPED state)

check_landing = ShortCircuitOperator(
    task_id="check_landing_has_files",
    python_callable=_check_landing_has_files,
)
```

**BranchPythonOperator vs ShortCircuitOperator:**

| Feature | BranchPythonOperator | ShortCircuitOperator |
|---|---|---|
| Returns | task_id string | bool |
| False/skip effect | Non-chosen branches skipped | ALL downstream tasks skipped |
| Use case | Pick one of N paths | Abort the whole pipeline if condition not met |
| Convergence needed? | Yes, with special trigger_rule | No — whole downstream is skipped |

---

### SLA Monitoring

**File:** `airflow/dags/dq_row_count_sla_check.py`

```python
def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    # Called by Airflow when any task in this DAG misses its SLA
    # task_list = list of Task objects that missed SLA
    # slas = list of SlaMiss records with details
    print(f"SLA MISSED for tasks: {[t.task_id for t in task_list]}")
    # Send alert to Slack/Discord...

with DAG(
    ...,
    sla_miss_callback=on_sla_miss,
    # DAG-level: fires if ANY task in the DAG misses its SLA
) as dag:

    get_row_count = PythonOperator(
        task_id="get_row_count",
        python_callable=_get_row_count,
        sla=timedelta(minutes=30),
        # This task MUST complete within 30 minutes of the DAG run's scheduled time
        # If it doesn't, on_sla_miss is called
        # Note: SLA miss is a WARNING, not a failure — the task keeps running
    )
```

**`depends_on_past=True`:**

```python
default_args = {
    "depends_on_past": True,
    # Today's run of this DAG won't start if YESTERDAY's run failed
    # Prevents cascading failures during a data outage:
    # - Monday: source DB is down, task fails
    # - Tuesday: with depends_on_past=True, Tuesday's run stays pending
    # - You fix the source, manually clear Monday's failed run
    # - Tuesday's run then starts
    # Without this: Monday fails, Tuesday runs anyway and also fails, etc.
}
```

---

### XCom Chaining with Parallel Tasks

**File:** `airflow/dags/dq_event_type_distribution.py`

```python
def _compute_today_distribution(**context):
    # Query: how many events of each type today?
    dist = {"purchase": 150, "view": 3200, "add_to_cart": 420}
    context["ti"].xcom_push(key="today_dist", value=dist)

def _compute_baseline_distribution(**context):
    # Query: how many events of each type yesterday? (baseline)
    baseline = {"purchase": 145, "view": 3100, "add_to_cart": 410}
    context["ti"].xcom_push(key="baseline_dist", value=baseline)

def _compare_distributions(**context):
    today    = context["ti"].xcom_pull(task_ids="compute_today",    key="today_dist")
    baseline = context["ti"].xcom_pull(task_ids="compute_baseline", key="baseline_dist")
    # Both tasks ran in parallel — we read both results here

    for event_type in today:
        today_val    = today.get(event_type, 0)
        baseline_val = baseline.get(event_type, 0)
        if baseline_val > 0:
            change_pct = abs(today_val - baseline_val) / baseline_val * 100
            if change_pct > 20:   # 20% drift threshold
                print(f"DRIFT: {event_type} changed {change_pct:.1f}%")

# DAG structure — two tasks run in PARALLEL, then fan-in
ensure_audit_table >> [compute_today, compute_baseline] >> compare_distributions
```

---

### Window Functions in PostgresOperator

**File:** `airflow/dags/dq_duplicate_detection.py`

```python
identify_duplicates = PostgresOperator(
    task_id="identify_duplicates",
    postgres_conn_id="postgres_default",
    # Connection defined in Airflow UI: Admin → Connections
    # postgres_default: host=postgres-db, port=5432, schema=analytics_platform
    sql="""
        INSERT INTO dq_duplicate_staging (event_id, window_start, rn)
        SELECT
            id,
            '{{ data_interval_start }}'::timestamptz AS window_start,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, event_type, DATE_TRUNC('second', created_at)
                -- PARTITION BY: group rows with same user + event_type + second together
                -- Within each group, ROW_NUMBER assigns 1 to the first row, 2 to the next, etc.
                ORDER BY id
                -- Lowest id = the ORIGINAL; higher ids = the duplicates
            ) AS rn
        FROM raw_events
        WHERE created_at >= '{{ data_interval_start }}'
          AND created_at <  '{{ data_interval_end }}';
    """,
)

# Later task: delete the duplicates (rn > 1 = not the original)
delete_duplicates = PostgresOperator(
    sql="DELETE FROM dq_duplicate_staging WHERE rn > 1;"
)
```

**How `ROW_NUMBER() OVER (PARTITION BY ...)` works:**

Imagine these rows in raw_events:

```
id=1, user_id=42, event_type=purchase, created_at=14:00:00.100
id=2, user_id=42, event_type=purchase, created_at=14:00:00.200  ← same second
id=3, user_id=42, event_type=purchase, created_at=14:00:00.900  ← same second
```

After `DATE_TRUNC('second', ...)` all three round to 14:00:00. They form one partition. `ROW_NUMBER()` assigns: id=1 → rn=1, id=2 → rn=2, id=3 → rn=3. We keep rn=1 and delete rn=2 and rn=3.

---

## 6. Reporting DAGs

### Jinja Date Macros Reference

```python
# In PostgresOperator sql= or BashOperator bash_command=:

'{{ ds }}'               # "2026-06-18"    — this run's logical date
'{{ yesterday_ds }}'     # "2026-06-17"    — the day before this run
'{{ next_ds }}'          # "2026-06-19"    — the day after
'{{ macros.ds_add(ds, -7) }}'   # "2026-06-11"    — 7 days ago

# datetime objects (have methods):
{{ data_interval_start.isoformat() }}   # "2026-06-18T14:00:00+00:00"
{{ data_interval_start.strftime('%Y/%m/%d') }}  # "2026/06/18"
```

---

### Daily Event Summary with Idempotent Upsert

**File:** `airflow/dags/daily_event_summary_report.py`

```python
populate_summary = PostgresOperator(
    sql="""
        INSERT INTO daily_event_summary (report_date, event_hour, event_type, event_count)
        SELECT
            '{{ yesterday_ds }}'::DATE                    AS report_date,
            EXTRACT(HOUR FROM created_at)::SMALLINT       AS event_hour,
            event_type,
            COUNT(*)                                      AS event_count
        FROM raw_events
        WHERE created_at >= '{{ yesterday_ds }} 00:00:00+00'
          AND created_at <  '{{ ds }} 00:00:00+00'
        GROUP BY 1, 2, 3
        ON CONFLICT (report_date, event_hour, event_type)
        DO UPDATE SET
            event_count = EXCLUDED.event_count,
            updated_at  = NOW();
        -- ON CONFLICT: if a row with the same (report_date, event_hour, event_type) exists...
        -- DO UPDATE: ...overwrite it with the new value
        -- EXCLUDED: refers to the row we TRIED to insert (the new data)
        -- This makes the task idempotent: running it twice updates, not duplicates
    """,
)
```

---

### Weekly Cohort Report with macros.ds_add

**File:** `airflow/dags/weekly_user_cohort_report.py`

```python
populate_cohort_report = PostgresOperator(
    sql="""
        INSERT INTO weekly_cohort_summary (cohort_week, report_week, event_type, event_count)
        SELECT
            uc.cohort_week,
            DATE_TRUNC('week', '{{ ds }}'::DATE)::DATE   AS report_week,
            re.event_type,
            COUNT(*)
        FROM raw_events re
        JOIN users u        ON u.id = re.user_id
        JOIN user_cohorts uc ON uc.user_id = u.id
        WHERE re.created_at >= '{{ macros.ds_add(ds, -7) }} 00:00:00+00'
        --                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^
        --   macros.ds_add(ds, -7) = 7 days before today's date
        --   For a daily schedule, this gives you the 7-day lookback window
          AND re.created_at <  '{{ ds }} 00:00:00+00'
        GROUP BY 1, 2, 3
        ON CONFLICT ... DO UPDATE ...
    """,
)
```

---

### TriggerDagRunOperator

**File:** `airflow/dags/monthly_revenue_kpi_report.py`

```python
trigger_archive = TriggerDagRunOperator(
    task_id="trigger_archive_dag",
    trigger_dag_id="archive_and_purge_users",
    # The dag_id of the DAG to trigger — must exist in Airflow

    wait_for_completion=False,
    # False = fire-and-forget: trigger and move on immediately
    # True  = block until the triggered DAG finishes (success or failure)

    conf={
        "triggered_by": "monthly_revenue_kpi_report",
        "month": "{{ ds }}",
        # conf is a templated field — Jinja works here
        # The triggered DAG can read conf via: context["dag_run"].conf.get("month")
    },

    trigger_rule="none_failed_min_one_success",
    # This task is after a branch convergence — need this rule so it fires
    # even when one of the upstream branches was skipped
)
```

**`wait_for_completion` comparison:**

| Setting | Behavior | Use case |
|---|---|---|
| `False` | Trigger and immediately move to next task | Independent side-tasks (archival, notifications) |
| `True` | Block until triggered DAG finishes | Sequential dependency — next step needs triggered DAG's output |

---

### Dynamic Task Generation

**File:** `airflow/dags/tenant_aggregated_report.py`

```python
TENANTS = ["nike", "puma", "adidas"]
# Defined at module level — this is the list of tenants to process

aggregate_tasks = []

for tenant in TENANTS:
    # This for-loop runs at DAG PARSE TIME (when Airflow loads the file)
    # NOT at task execution time
    # So the number of tasks is fixed once the DAG is loaded

    task = PythonOperator(
        task_id=f"aggregate_{tenant}",
        # task_ids must be unique within a DAG
        # f-string ensures: "aggregate_nike", "aggregate_puma", "aggregate_adidas"

        python_callable=_aggregate_tenant,
        # Same function for all three — differentiated by op_kwargs

        op_kwargs={"tenant": tenant},
        # Passes "tenant" as a keyword argument to _aggregate_tenant
        # Inside the function: def _aggregate_tenant(tenant, **context)
    )
    aggregate_tasks.append(task)

start >> aggregate_tasks         # fan-out: start → [nike, puma, adidas] in parallel
aggregate_tasks >> merge_results # fan-in: merge waits for all three
```

**Interview answer:**
> "The for-loop runs when Airflow imports the Python file — at parse time, not runtime. So the task graph is static once the DAG is loaded. Adding a tenant requires restarting the scheduler to re-parse the file. For truly dynamic task counts based on runtime data, you'd use Airflow 2.3+'s dynamic task mapping with `.expand()`."

---

## 7. Advanced Orchestration DAGs

### ExternalTaskSensor

**File:** `airflow/dags/pipeline_health_monitor.py`

```python
sense_batch_etl = ExternalTaskSensor(
    task_id="sense_batch_etl_completed",
    external_dag_id="batch_etl_pipeline_v1",
    # Watch this DAG

    external_task_id="spark_transform_landing_to_gold",
    # Watch this specific task within that DAG
    # If None: wait for the entire DAG run to complete

    execution_delta=timedelta(hours=0),
    # Match the run with the SAME logical date as this sensor's run
    # execution_delta=timedelta(hours=1) would look at the previous hour's run

    timeout=3600,
    # Give up and fail (or skip if soft_fail=True) after 1 hour

    poke_interval=120,
    # Check the external task's state every 2 minutes

    mode="reschedule",
    # CRITICAL — explained below

    allowed_states=["success"],
    # Only proceed if the external task is in "success" state

    soft_fail=True,
    # If the external task/run is not found (e.g. batch ETL never ran today),
    # mark this sensor as SKIPPED instead of FAILED
    # Prevents health monitor from alerting on days when batch ETL is paused
)
```

**Why `mode="reschedule"` is critical:**

```
LocalExecutor worker slots (e.g. 4 slots):

mode="poke":
  Slot 1: [sensor] sleeping...sleeping...sleeping...  ← BLOCKED for hours
  Slot 2: [sensor] sleeping...sleeping...sleeping...  ← BLOCKED for hours
  Slot 3: [sensor] sleeping...sleeping...sleeping...  ← BLOCKED for hours
  Slot 4: [sensor] sleeping...sleeping...sleeping...  ← BLOCKED for hours
  Result: NO other tasks can run — DEADLOCK

mode="reschedule":
  Slot 1: [sensor checks] → releases slot → [other task runs] → [sensor re-checks]
  Slot 2: free for other tasks
  Slot 3: free for other tasks
  Slot 4: free for other tasks
  Result: sensors and other tasks share slots gracefully
```

**Interview answer:**
> "LocalExecutor runs tasks as subprocesses on the same machine. `mode='poke'` holds a worker slot in a sleep loop — one sensor blocks one slot indefinitely. With 4 worker slots and 4 sensors, nothing else can run. `mode='reschedule'` releases the slot between checks; the sensor goes back to the scheduler queue and is rescheduled after the interval."

---

### Self-Aware Pipeline

**File:** `airflow/dags/adaptive_schedule_etl.py`

```python
def _evaluate_recent_run_history(**context):
    """
    The DAG reads its OWN past run history to decide how to behave.
    This is the "self-aware pipeline" pattern.
    """
    async def _query():
        conn = await asyncpg.connect(db_url)
        rows = await conn.fetch("""
            SELECT run_date, row_count, mode
            FROM pipeline_run_log
            WHERE dag_id = 'adaptive_schedule_etl'
            ORDER BY run_date DESC
            LIMIT 3          -- look at the last 3 runs
        """)
        return [dict(r) for r in rows]

    recent_runs = asyncio.run(_query())
    # asyncio.run() bridges sync PythonOperator and async asyncpg

    if len(recent_runs) < 3:
        needs_full = False
        # Not enough history — run normally
    else:
        all_low = all(r["row_count"] < 10 for r in recent_runs)
        # all() returns True only if EVERY element is True
        needs_full = all_low

    context["ti"].xcom_push(key="needs_full_reprocess", value=needs_full)


def _branch_full_vs_incremental(**context):
    needs_full = context["ti"].xcom_pull(
        task_ids="evaluate_recent_run_history",
        key="needs_full_reprocess"
    )
    return "run_full_reprocess" if needs_full else "run_incremental_load"


# Branch A: Full reprocess — reprocesses the last 24 hours
run_full_reprocess = BashOperator(
    bash_command=(
        # Shell date arithmetic (NOT Jinja macros — macros unavailable at parse time in this context)
        "YESTERDAY=$(date -u -d '{{ ds }} -1 day' '+%Y-%m-%dT00:00:00+00:00') && "
        "spark-submit ... --start $YESTERDAY --end {{ ds }}T00:00:00+00:00"
    ),
)

# Branch B: Standard incremental — processes only this 1-hour window
run_incremental_load = BashOperator(
    bash_command=(
        "spark-submit ... "
        "--start {{ data_interval_start.isoformat() }} "
        "--end {{ data_interval_end.isoformat() }}"
    ),
)
```

---

### Sequential Multi-DAG Backfill

**File:** `airflow/dags/conditional_spark_backfill_orchestrator.py`

```python
def _scan_gold_for_missing_dates(**context):
    run_date = datetime.strptime(context["ds"], "%Y-%m-%d")
    missing_dates = []

    for days_ago in range(1, 8):   # check last 7 days
        check_date = run_date - timedelta(days=days_ago)
        year, month, day = check_date.year, check_date.month, check_date.day

        pattern = f"/data/landing/{year:04d}/{month:02d}/{day:02d}/**/*.parquet"
        files = glob.glob(pattern, recursive=True)

        if not files:
            missing_dates.append(check_date.strftime("%Y-%m-%d"))

    context["ti"].xcom_push(key="missing_dates", value=missing_dates)


# Dynamic task generation: create 7 TriggerDagRunOperator tasks at parse time
trigger_tasks = []
prev_task = prepare_backfill_plan

for day_offset in range(7, 0, -1):   # 7, 6, 5, 4, 3, 2, 1 (oldest first)
    trigger_task = TriggerDagRunOperator(
        task_id=f"trigger_backfill_day_minus_{day_offset}",
        trigger_dag_id="batch_etl_pipeline_v1",

        wait_for_completion=True,
        # BLOCK until the triggered batch ETL run finishes
        # This creates a sequential chain:
        #   day 7 finishes → day 6 starts → day 6 finishes → day 5 starts → ...
        # Ensures chronological order of backfill

        reset_dag_run=True,
        # If a run already exists for this date (previous failed attempt),
        # reset it so TriggerDagRunOperator can create a fresh run
    )

    prev_task >> trigger_task   # chain: prepare → trigger7 → trigger6 → ... → trigger1
    trigger_tasks.append(trigger_task)
    prev_task = trigger_task    # next loop iteration chains from this task
```

---

### Parallel Sensor Fan-In (AND-Join)

**File:** `airflow/dags/multi_sensor_coordination_pipeline.py`

```python
sense_batch_etl    = ExternalTaskSensor(task_id="sense_batch_etl_gold",    ...)
sense_nike_tenant  = ExternalTaskSensor(task_id="sense_nike_tenant_etl",   ...)

all_upstream_ready = EmptyOperator(task_id="all_upstream_ready")
# default trigger_rule = "all_success"
# This means: run only when ALL incoming edges are in "success" state
# Since both sensors must succeed → this is an AND-join

[sense_batch_etl, sense_nike_tenant] >> all_upstream_ready
# List >> Task syntax creates edges:
#   sense_batch_etl   → all_upstream_ready
#   sense_nike_tenant → all_upstream_ready
# Both must succeed before all_upstream_ready runs
```

---

## 8. Data Lifecycle DAGs

### VACUUM via asyncpg (not PostgresOperator)

**File:** `airflow/dags/cleanup_old_live_metrics.py`

```python
async def _vacuum():
    db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
    # DATABASE_URL = "postgresql+asyncpg://user:pass@host/db"  (SQLAlchemy format)
    # asyncpg.connect() needs: "postgresql://user:pass@host/db"
    # .replace() strips the "+asyncpg" dialect suffix

    conn = await asyncpg.connect(db_url)
    # asyncpg is a pure-Python async Postgres driver
    # Much faster than psycopg2 for I/O-bound workloads

    await conn.execute("VACUUM ANALYZE live_event_metrics")
    # VACUUM:
    #   Postgres uses MVCC (Multi-Version Concurrency Control)
    #   When you UPDATE or DELETE a row, Postgres marks the old version as "dead"
    #   but doesn't immediately free the space (other transactions might still see it)
    #   VACUUM physically removes dead row versions and reclaims disk space
    #
    # ANALYZE:
    #   Updates the query planner's statistics about table contents
    #   Without this, the planner might choose bad query plans (e.g. seq scan instead of index)
    #
    # CRITICAL: VACUUM cannot run inside a transaction block
    # PostgresOperator wraps ALL SQL in BEGIN ... COMMIT
    # So you CANNOT use PostgresOperator for VACUUM — it will raise an error
    # asyncpg.connect() is NOT in a transaction by default — this is why we use it

asyncio.run(_vacuum())
# asyncio.run() runs the async coroutine synchronously
# PythonOperator expects a regular sync function
# asyncio.run() is the bridge between sync Airflow and async asyncpg
```

---

### Guard-Before-Delete Pattern

**File:** `airflow/dags/archive_and_purge_users.py`

```python
# The problem: never delete data without backing it up first
# The solution: export → verify → delete (in that order, with ShortCircuit guard)

export_to_parquet = PythonOperator(
    task_id="export_inactive_users_to_parquet",
    python_callable=_export_to_parquet,
    # Writes: /data/gold/archived_users/2026-06-18/users.parquet
)

verify_export = ShortCircuitOperator(
    task_id="verify_export_file_exists",
    python_callable=_verify_parquet_exists,
    # Returns True if the file exists and has rows
    # Returns False if export failed silently → all downstream tasks SKIPPED
    # The delete never happens without a confirmed backup
)

soft_delete = PostgresOperator(
    task_id="soft_delete_users",
    sql="""
        UPDATE users
        SET deleted_at = NOW()         -- mark as deleted with timestamp
        WHERE last_active < NOW() - INTERVAL '365 days'
          AND deleted_at IS NULL;      -- only process once
        -- Soft delete: set deleted_at, don't actually DELETE the row
        -- Data is still in the table and recoverable
        -- A hard DELETE is permanent — prefer soft deletes for user data
    """,
)

export_to_parquet >> verify_export >> soft_delete
# ShortCircuit prevents soft_delete if the export failed
```

---

### Batched Deletes

```python
def _delete_old_metrics(**context):
    cutoff = context["data_interval_start"] - timedelta(days=RETENTION_DAYS)

    async def _delete():
        conn = await asyncpg.connect(db_url)
        deleted_total = 0

        while True:
            result = await conn.execute("""
                DELETE FROM live_event_metrics
                WHERE id IN (
                    SELECT id FROM live_event_metrics
                    WHERE window_start < $1
                    LIMIT 1000         -- delete 1000 rows at a time
                )
            """, cutoff)
            # Why batch deletes?
            # Deleting millions of rows in one statement holds a huge lock
            # Other queries (reads/writes) are blocked for the entire duration
            # 1000-row batches = brief locks, other queries can run between batches

            deleted = int(result.split()[-1])   # "DELETE 1000" → 1000
            deleted_total += deleted
            if deleted < 1000:
                break   # last batch had fewer than 1000 = we're done
```

---

## 9. Monitoring DAGs

### BashOperator XCom via stdout

**File:** `airflow/dags/kafka_consumer_lag_monitor.py`

```python
fetch_lag = BashOperator(
    task_id="fetch_consumer_lag",
    bash_command=(
        "kafka-consumer-groups.sh "
        "--bootstrap-server kafka-broker:29092 "
        "--describe --group spark-streaming-consumer "
        # Output format: GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  ...
        "| awk 'NR>1 {sum += $5} END {print sum+0}'"
        # NR>1: skip header row (NR = Number of Records processed)
        # $5: 5th column = LAG per partition
        # sum += $5: accumulate total lag
        # print sum+0: print total ('+0' converts empty string to 0)
        "|| echo -1 "
        # If the kafka command fails (Kafka down), output -1 as a sentinel value
        # Without this, a Kafka failure would fail the whole DAG
    ),
    do_xcom_push=True,
    # BashOperator captures the LAST LINE of stdout
    # and pushes it to XCom under key "return_value"
    # Here, the last line is the total lag number (e.g. "3450" or "-1")
)

def _alert_if_lag_high(**context):
    lag_str = context["ti"].xcom_pull(
        task_ids="fetch_consumer_lag",
        key="return_value"     # "return_value" = BashOperator's default XCom key
    )
    lag = int(lag_str or -1)

    if lag == -1:
        print("Kafka unreachable — skipping lag check")
        return
    if lag > LAG_THRESHOLD:
        print(f"HIGH LAG: {lag} messages behind")
        # Send alert
```

---

### Querying Airflow's Own Database

**File:** `airflow/dags/failed_task_audit_reporter.py`

```python
conn_str = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
# This env var is set by Airflow itself — it's the connection string Airflow uses
# for its own metadata database. Format: postgresql+psycopg2://user:pass@host:port/db

db_url = conn_str.replace("postgresql+psycopg2://", "postgresql://")
# asyncpg uses plain postgresql:// format, not the SQLAlchemy dialect format

conn = await asyncpg.connect(db_url)

rows = await conn.fetch("""
    SELECT
        dag_id,
        task_id,
        state,
        start_date,
        end_date,
        (EXTRACT(EPOCH FROM (end_date - start_date)))::INT AS duration_seconds
    FROM task_instance
    -- This is Airflow's internal table storing every task execution
    WHERE state = 'failed'
      AND start_date >= NOW() - INTERVAL '24 hours'
    ORDER BY start_date DESC
""")
# This is READ-ONLY — we're not modifying Airflow's internal state
# Safe to do while Airflow is running
```

**Interview answer:**
> "Airflow stores all orchestration history in its metadata Postgres database — the same one we use for analytics. The `task_instance` table has every task execution ever. By reading it directly from a PythonOperator, we can build failure reports without any external monitoring tool. We're careful to only SELECT — never INSERT/UPDATE Airflow's internal tables."

---

## 10. Dynamic DAG Factory

**File:** `airflow/dags/dynamic_dag_factory.py`

### Config file

**File:** `airflow/config/central_tenant_manifest.json`

```json
[
  {
    "client_id": "nike",
    "schedule": "0 * * * *",
    "target_table": "analytics_nike",
    "retention_days": 90
  },
  {
    "client_id": "puma",
    "schedule": "0 */2 * * *",
    "target_table": "analytics_puma",
    "retention_days": 60
  }
]
```

### DAG generation

```python
import json

# Read config at MODULE LOAD TIME (when Airflow imports this file)
with open("/opt/airflow/config/central_tenant_manifest.json") as f:
    TENANTS = json.load(f)
# This file is read ONCE per DAG file scan (every ~30 seconds by default)

for tenant in TENANTS:
    dag_id = f"dynamic_tenant_etl_{tenant['client_id']}"
    # e.g. "dynamic_tenant_etl_nike"

    # Create DAG object
    dag = DAG(
        dag_id=dag_id,
        schedule_interval=tenant["schedule"],
        start_date=datetime(2026, 1, 1),
        catchup=False,
    )

    # Add tasks to the DAG
    with dag:
        extract = PythonOperator(
            task_id="extract_tenant_data",
            python_callable=_extract_for_tenant,
            op_kwargs={"tenant": tenant},
        )
        spark_job = BashOperator(
            task_id="execute_tenant_spark_job",
            bash_command=f"spark-submit ... --tenant {tenant['client_id']}",
        )
        extract >> spark_job

    # CRITICAL: assign to globals() so Airflow can discover this DAG
    globals()[dag_id] = dag
    # Airflow scans module-level variables for DAG objects
    # globals() is the module's global variable dictionary
    # globals()["dynamic_tenant_etl_nike"] = dag_object
    # → Airflow finds it as if you had written: dynamic_tenant_etl_nike = DAG(...)
```

**Interview answer:**
> "Airflow discovers DAGs by importing each Python file and looking for objects of type `DAG` in the module's global scope. `globals()[dag_id] = dag` programmatically adds a DAG to that scope. Adding a new tenant requires only a new JSON entry — no Python changes, no deployment, just save the JSON file. Airflow re-reads it on the next DAG scan (within ~30 seconds)."

---

## 11. Spark Jobs

**File:** `spark/apps/metrics_aggregation.py`

### SparkSession

```python
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--start")   # e.g. "2026-06-18T14:00:00+00:00"
parser.add_argument("--end")
args = parser.parse_args()

spark = SparkSession.builder \
    .appName("MetricsAggregation") \
    # Name shown in Spark Web UI (http://localhost:8082)
    .getOrCreate()
    # .getOrCreate() — if a SparkSession already exists in this JVM, reuse it
    # Otherwise create a new one
    # When submitted via spark-submit, Spark initialises the session automatically
```

### Reading from the data lake

```python
df = spark.read.parquet(f"/data/landing/{year}/{month}/{day}/{hour}/")
# Spark reads ALL .parquet files in this directory in PARALLEL
# Each file becomes one or more partitions, distributed across workers
# Schema is inferred from the Parquet metadata (Parquet is self-describing)

df.printSchema()
# root
#  |-- id: long (nullable = true)
#  |-- user_id: long (nullable = true)
#  |-- event_type: string (nullable = true)
#  |-- created_at: timestamp (nullable = true)
```

### Transformations (lazy evaluation)

```python
# All of these are LAZY — they build a logical plan, not execute yet
result = df \
    .filter(df.event_type.isNotNull()) \
    .groupBy("event_type", "product_id") \
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("user_id").alias("unique_users"),
    )
# Spark's Catalyst optimizer rewrites this plan for efficiency
# e.g. it might push the filter before the groupBy to reduce data early
```

### Writing to gold (triggers execution)

```python
result.write \
    .mode("overwrite") \
    # "overwrite" = delete existing files in this path, then write
    # Makes the job idempotent — retry gives the same result
    # Other modes: "append" (add to existing), "ignore" (skip if exists), "error" (fail if exists)
    .partitionBy("event_type") \
    # Creates subdirectories: /data/gold/.../event_type=purchase/part-00000.parquet
    # When reading this data later, Spark can skip entire directories
    # by pushing filters into the read (partition pruning)
    .parquet(f"/data/gold/{year}/{month}/{day}/{hour}/")
# .parquet() is an ACTION — it triggers the actual computation and write
# Only now does Spark execute the lazy plan above
```

**Interview answer for lazy evaluation:**
> "Spark uses lazy evaluation — transformations like `filter`, `groupBy`, `join` don't execute immediately. They build a logical execution plan. Only an action like `write`, `count`, or `collect` triggers actual computation. This lets the Catalyst optimizer reorder and combine operations before any data moves."

### Streaming consumer

**File:** `spark/apps/streaming_consumer.py`

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:29092") \
    .option("subscribe", "user-events") \
    # Subscribe to this Kafka topic
    .option("startingOffsets", "latest") \
    # "latest" = only process new messages arriving after this job starts
    # "earliest" = reprocess everything from the beginning of the topic
    .load()

# Kafka delivers each message as a row with columns:
# value (binary), key (binary), topic, partition, offset, timestamp

parsed = df.select(
    F.from_json(
        F.col("value").cast("string"),   # decode binary → string
        schema                            # parse JSON string → struct columns
    ).alias("data")
).select("data.*")
# data.* = expand the struct into individual columns

# Write to Postgres in micro-batches
query = parsed.writeStream \
    .foreachBatch(_write_batch_to_postgres) \
    # Call our function with each micro-batch DataFrame
    .option("checkpointLocation", "/data/checkpoints/streaming/") \
    # Checkpoint = save the last processed Kafka offset to disk
    # If the job restarts, it resumes from where it left off
    # Without checkpointing, restart = reprocess from startingOffsets
    .trigger(processingTime="30 seconds") \
    # Collect 30 seconds of events, then process as one batch
    .start()

query.awaitTermination()
# Keep the streaming job running forever (until killed)
```

---

## 12. Key Interview Q&A

### Architecture & Design

**Q: Why is Airflow not in the real-time ingestion path?**
> "Airflow's scheduler adds latency — it checks for tasks to run on a configurable interval (default 5 seconds). For a real-time path where you want sub-second response, that's unacceptable. Kafka is built for high-throughput, low-latency message passing. We let Kafka handle all real-time ingestion and use Airflow only for scheduled batch jobs where the latency is measured in minutes, not milliseconds."

**Q: What is the difference between LocalExecutor, CeleryExecutor, and KubernetesExecutor?**
> "LocalExecutor runs tasks as subprocesses on the same machine as the Airflow scheduler. Simple, no extra infrastructure, limited to one machine's resources. CeleryExecutor distributes tasks to worker machines via a message broker (Redis or RabbitMQ) — horizontal scaling but more infrastructure. KubernetesExecutor launches a fresh Kubernetes pod for each task — perfect isolation and scaling, but cold-start latency per task. We use LocalExecutor here because it's a single-machine learning environment."

**Q: What is the lakehouse pattern?**
> "Three layers: Landing (raw, immutable Parquet), Bronze (validated), Gold (aggregated). Each layer is a recovery checkpoint. If gold is wrong, reprocess from bronze. If bronze is wrong, reprocess from landing. You never need to re-hit the source system."

---

### Airflow Concepts

**Q: What is idempotency and how do you achieve it in Airflow?**
> "Idempotency means running a task multiple times produces the same result. We achieve it three ways: (1) use `data_interval_start/end` to query a fixed time window — retries read the same rows, (2) write to partitioned Parquet paths that get overwritten on retry, (3) use `ON CONFLICT DO UPDATE` in SQL inserts so retries update rather than duplicate rows."

**Q: What is the difference between BranchPythonOperator and ShortCircuitOperator?**
> "BranchPythonOperator returns a task_id string — it picks one of multiple parallel paths to run, skipping the others. ShortCircuitOperator returns a bool — False skips ALL downstream tasks entirely. Use Branch for 'which path to take', use ShortCircuit for 'should we even bother running'."

**Q: What trigger_rule do you need after a branch convergence and why?**
> "`none_failed_min_one_success`. The default `all_success` requires every upstream task to succeed — but one branch is always SKIPPED, and SKIPPED doesn't count as success. `none_failed_min_one_success` fires when at least one upstream succeeded and none failed, which is exactly the case after branching."

**Q: Why use `mode='reschedule'` on sensors?**
> "LocalExecutor has a fixed number of worker slots. `mode='poke'` occupies a slot in a sleep loop — one sensor can block all other tasks from running. `mode='reschedule'` releases the slot between checks. The sensor is rescheduled by Airflow after the interval, freeing the slot for other tasks in between."

**Q: What are XComs and what are their limits?**
> "XCom (cross-communication) stores key-value pairs in Airflow's Postgres metadata database. Any task can push and pull values. The practical size limit is ~48KB because it's stored in Postgres. For larger data, write to S3 or Postgres and pass the path/key via XCom."

**Q: What is `depends_on_past=True`?**
> "Today's run won't start if yesterday's run failed. This prevents cascade failures during outages — if Monday's extraction fails, Tuesday stays pending rather than running and also failing. You fix the root cause, clear Monday's failed run, and Tuesday proceeds."

**Q: Why can't VACUUM run inside a PostgresOperator?**
> "PostgresOperator wraps all SQL in a `BEGIN ... COMMIT` transaction. VACUUM cannot run inside a transaction — it's a statement that must run outside transaction context. We use asyncpg directly, which does NOT auto-wrap in a transaction. That's the only way to VACUUM from an Airflow task."

**Q: How does dynamic DAG generation work?**
> "Airflow discovers DAGs by importing each Python file and scanning the module's global namespace for `DAG` objects. `globals()[dag_id] = dag_object` programmatically adds a DAG to that namespace. The for-loop runs at parse time, not execution time. Adding a new tenant requires only a JSON config change."

---

### Kafka Concepts

**Q: What is KRaft mode?**
> "In traditional Kafka, ZooKeeper managed cluster metadata — leader elections, topic configs, broker registrations. This required running two separate systems. KRaft (Kafka Raft) replaces ZooKeeper with a built-in Raft consensus implementation inside Kafka itself. One fewer component to manage, simpler operations."

**Q: What is a consumer group and consumer lag?**
> "A consumer group is a set of consumers that collaborate to read from a topic — each partition is assigned to exactly one consumer in the group. Consumer lag is the difference between the latest message offset in a partition and the offset the consumer has processed. High lag means the consumer is falling behind — a sign of insufficient processing capacity or a stuck consumer."

**Q: What is the Dead Letter Queue (DLQ)?**
> "When an event can't be processed (malformed JSON, schema violation, processing error), instead of dropping it or retrying forever, we route it to a separate 'dead letter' topic (`user-events-dlq`). This preserves the data for investigation and replay. The `dlq_replay_pipeline` DAG periodically retries DLQ events after the root cause is fixed."

---

### Spark Concepts

**Q: What is lazy evaluation in Spark?**
> "Transformations like `filter`, `groupBy`, and `join` don't execute when you call them — they build a logical execution plan. Only actions like `write`, `count`, or `collect` trigger actual computation. This lets Spark's Catalyst optimizer reorder and combine operations before any data moves across the network."

**Q: What is a wide vs narrow transformation?**
> "Narrow transformations (filter, map, select) process each partition independently — no data movement. Wide transformations (groupBy, join, distinct) require data from multiple partitions to be combined — this causes a shuffle, which is the most expensive operation in Spark (data moves across the network between workers)."

**Q: What is partition pruning?**
> "When you write Parquet files with `.partitionBy('event_type')`, Spark creates subdirectories like `event_type=purchase/`. When you later read and filter by event_type, Spark reads ONLY the matching subdirectory — it never reads other partitions. This is called partition pruning and can reduce read time by 90%+ on large datasets."

**Q: What is `deploy-mode client` vs `cluster`?**
> "In `client` mode, the Spark driver (the coordinator process) runs on the machine that submitted the job — the Airflow worker container. In `cluster` mode, the driver runs on one of the Spark worker nodes. Client mode is simpler for debugging (logs come back to the submitting machine). Cluster mode is used in production because the driver doesn't depend on the submitting machine staying alive."

---

### PostgreSQL Concepts

**Q: What is MVCC and why does VACUUM exist?**
> "Multi-Version Concurrency Control — Postgres never overwrites a row in place. When you UPDATE a row, Postgres writes a new version and marks the old one as 'dead'. This allows readers to see a consistent snapshot without blocking writers. The downside: dead row versions accumulate on disk. VACUUM physically removes them and reclaims space."

**Q: What is `ON CONFLICT DO UPDATE`?**
> "An upsert — if the INSERT would violate a unique constraint, instead of failing, execute the UPDATE clause. `EXCLUDED` refers to the values in the row that was attempted. This is the standard pattern for idempotent writes: run it 10 times, the final state is the same as running it once."

**Q: Why use composite indexes?**
> "A composite index on `(created_at, event_type)` covers queries that filter on both columns. Postgres can use the index to jump directly to matching rows without scanning the entire table. Column order matters — the index is efficient for queries on `created_at` alone, or `(created_at, event_type)` together, but NOT for queries on `event_type` alone (leftmost prefix rule)."

---

*End of FlowStack Interview Preparation Guide*
*Generated: 2026-06-18 | FlowStack repo: github.com/Utkarsh-1307/Flowstack*
