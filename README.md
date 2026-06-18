# FlowStack — Real-Time ETL & Analytics Platform

A production-grade data engineering platform built from scratch. Covers real-time stream ingestion, distributed batch computation, workflow orchestration, data quality, monitoring, and multi-tenant reporting — all driven by Apache Airflow.

---

## Architecture

```
[ Apache Kafka ]
        │
   ┌────┴────────────────────┐
   ▼                         ▼
[ Spark Structured       [ Airflow Scheduler ]
  Streaming ]                │
   │                         │ Triggers
   ▼                         ▼
[ Postgres            [ Spark Batch Job ]
  Live Metrics ]             │
                             ▼
                    [ Postgres Gold Layer ]
                             │
                    [ /data/ Lake ]
                    landing/ → bronze/ → gold/
```

**Key design decision:** Airflow is never in the synchronous ingestion path. Kafka absorbs all incoming traffic; Airflow only orchestrates scheduled batch jobs and data quality checks.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Queue | Apache Kafka (KRaft, no Zookeeper) |
| Stream Processing | Apache Spark Structured Streaming |
| Batch Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Distributed Compute | Apache Spark 3.5 (1 master + 2 workers) |
| Database | PostgreSQL 15 |
| Infrastructure | Docker Compose |
| CI/CD | GitHub Actions |

---

## Project Structure

```
FlowStack/
├── .github/workflows/ci.yml          # CI: DAG validation, Spark lint
├── airflow/
│   ├── dags/                         # 26 production DAGs (see catalogue below)
│   ├── config/
│   │   └── central_tenant_manifest.json  # Config-driven multi-tenant DAG factory
│   └── plugins/alerts.py             # Slack/Discord failure hooks
├── spark/
│   ├── apps/
│   │   ├── metrics_aggregation.py    # Batch: landing → gold aggregation
│   │   ├── streaming_consumer.py     # Streaming: Kafka → Postgres live metrics
│   │   └── tenant_job.py             # Multi-tenant batch processor
│   └── core/                         # Shared schemas + validation helpers
├── docker/
│   ├── postgres/init.sql             # Schema, indexes (composite + GIN)
│   └── spark-cluster/                # Spark tuning config
├── data/
│   ├── landing/                      # Raw Parquet dumps (immutable, hourly)
│   ├── bronze/                       # Validated records
│   └── gold/                         # Aggregated analytics output
└── docker-compose.yml
```

---

## Quickstart

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### 1. Clone & configure

```bash
git clone https://github.com/Utkarsh-1307/Flowstack.git
cd Flowstack
cp .env.example .env
```

### 2. Build & start

```bash
docker compose up --build -d
```

First run takes ~10–15 minutes (downloading Spark, Airflow, Kafka images). Subsequent starts take under 30 seconds.

### 3. Open the services

| Service | URL | Credentials |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | `admin` / `admin` |
| **Spark Master UI** | http://localhost:8082 | — |
| **Postgres** | `localhost:5433` | `engine_admin` / see `.env` |

### 4. Verify DAGs loaded

```bash
docker exec airflow_scheduler airflow dags list
docker exec airflow_scheduler airflow dags list-import-errors
```

Expected: 26 DAGs listed, 0 import errors.

---

## DAG Catalogue

### Core ETL (2 DAGs)

| DAG | Schedule | Description |
|---|---|---|
| `batch_etl_pipeline` | Hourly | Idempotent extract from Postgres → Parquet landing → Spark gold. The foundation DAG. |
| `dynamic_dag_factory` | Per-tenant | Reads `central_tenant_manifest.json` and auto-generates one DAG per tenant — no Python changes needed to add a client. |

### Data Quality (5 DAGs)

| DAG | Schedule | Description |
|---|---|---|
| `dq_null_check_pipeline` | Hourly | BranchPythonOperator: if null % > threshold → quarantine, else pass gate. |
| `dq_schema_drift_detector` | Daily | ShortCircuitOperator: compares current Parquet schema against a snapshot; alerts on column additions/removals/type changes. |
| `dq_row_count_sla_check` | Daily | `depends_on_past=True` + `sla=timedelta()`: enforces a minimum row count and fires an SLA miss callback if breached. |
| `dq_event_type_distribution` | Daily | XCom chaining: computes today's event-type distribution in parallel, then compares against yesterday's baseline. |
| `dq_duplicate_detection` | Hourly | PostgresOperator with `ROW_NUMBER() OVER (PARTITION BY ...)` window function to identify and stage duplicate events. |

### Reporting (4 DAGs)

| DAG | Schedule | Description |
|---|---|---|
| `daily_event_summary_report` | Daily 01:00 | Aggregates yesterday's events by hour + type into `daily_event_summary`. Uses `{{ yesterday_ds }}` / `{{ ds }}` Jinja macros. |
| `weekly_user_cohort_report` | Weekly Mon 06:00 | Joins raw_events → users → user_cohorts; uses `{{ macros.ds_add(ds, -7) }}` for the 7-day lookback window. |
| `monthly_revenue_kpi_report` | Monthly 1st 07:00 | Computes MoM revenue KPIs, then fires `TriggerDagRunOperator` (fire-and-forget) to archive old users. |
| `tenant_aggregated_report` | Daily 04:00 | Dynamic task generation: for-loop at parse time creates one `PythonOperator` per tenant — fan-out → fan-in. |

### Data Lifecycle (5 DAGs)

| DAG | Schedule | Description |
|---|---|---|
| `cleanup_landing_zone` | Daily 02:00 | Deletes Parquet files older than `LANDING_RETENTION_DAYS` (env var, default 30). ShortCircuit guard if empty. |
| `cleanup_old_live_metrics` | Daily 03:00 | Batched deletes from `live_event_metrics`; runs `VACUUM ANALYZE` via asyncpg autocommit (cannot run inside a transaction). |
| `bronze_to_gold_compaction` | Daily 00:30 | `depends_on_past=True`: compacts yesterday's bronze Parquet into optimised gold partitions. Logs to `compaction_manifest`. |
| `archive_and_purge_users` | On-trigger | Guard-before-delete pattern: exports inactive users to Parquet archive first, then soft-deletes. |
| `hourly_live_metrics_rollup` | Hourly | ShortCircuitOperator guards empty streaming windows; rolls up `live_event_metrics` into hourly summaries. |

### Monitoring (6 DAGs)

| DAG | Schedule | Description |
|---|---|---|
| `pipeline_health_monitor` | Every 3h | `ExternalTaskSensor` (mode="reschedule") waits for `batch_etl_pipeline` completion, then runs health checks. |
| `kafka_consumer_lag_monitor` | Every 30min | BashOperator stdout → XCom: runs `kafka-consumer-groups.sh`, parses lag, alerts if above threshold. |
| `spark_cluster_health_check` | Hourly | HTTP requests to Spark master REST API (`http://spark-master:8080/json/`); checks worker count and memory. |
| `data_freshness_sla_tracker` | Hourly | Measures time since last event in Postgres + age of newest gold Parquet file; alerts on stale data. |
| `failed_task_audit_reporter` | Daily 06:00 | Queries Airflow's own `task_instance` table (read-only) to report all failed tasks in the past 24 hours. |
| `event_ingestion_error_tracker` | Hourly | Bootstraps `ingestion_errors` table and aggregates error counts by type for observability. |

### Advanced Orchestration (4 DAGs)

| DAG | Schedule | Description |
|---|---|---|
| `dlq_replay_pipeline` | Every 6h | ShortCircuitOperator + Kafka consumer: replays events from the dead-letter queue, offset-safe. |
| `conditional_spark_backfill_orchestrator` | Daily 05:00 | Scans `/data/landing/` for missing date partitions, then triggers `TriggerDagRunOperator` per gap (`wait_for_completion=True`), sequentially oldest-first. |
| `multi_sensor_coordination_pipeline` | Every 3h | Two parallel `ExternalTaskSensor`s (AND-join): waits for both core ETL and Nike tenant ETL before running a cross-pipeline Spark join. |
| `adaptive_schedule_etl` | Hourly | Self-aware pipeline: reads its own `pipeline_run_log` history. If last 3 runs all had low row counts → full 24h reprocess; otherwise incremental 1h load. |

---

## Key Airflow Concepts Demonstrated

| Concept | Where |
|---|---|
| `BranchPythonOperator` — returns task_id string to pick a branch | `dq_null_check_pipeline`, `adaptive_schedule_etl`, `conditional_spark_backfill_orchestrator` |
| `ShortCircuitOperator` — returns bool; False → downstream SKIPPED | `dq_schema_drift_detector`, `cleanup_landing_zone`, `dlq_replay_pipeline` |
| `ExternalTaskSensor` with `mode="reschedule"` — critical for LocalExecutor | `pipeline_health_monitor`, `multi_sensor_coordination_pipeline` |
| `TriggerDagRunOperator` with `wait_for_completion=True/False` | `monthly_revenue_kpi_report`, `conditional_spark_backfill_orchestrator` |
| Dynamic task generation (for-loop at parse time) | `tenant_aggregated_report`, `conditional_spark_backfill_orchestrator` |
| `depends_on_past=True` — sequential run safety | `dq_row_count_sla_check`, `bronze_to_gold_compaction` |
| `sla=timedelta()` + `sla_miss_callback` | `dq_row_count_sla_check` |
| XCom chaining across tasks | `dq_event_type_distribution`, `kafka_consumer_lag_monitor` |
| Jinja macros: `{{ ds }}`, `{{ yesterday_ds }}`, `{{ macros.ds_add() }}` | Reporting DAGs, `batch_etl_pipeline` |
| `trigger_rule="none_failed_min_one_success"` for branch convergence | All branching DAGs |
| asyncpg autocommit for `VACUUM ANALYZE` | `cleanup_old_live_metrics` |
| `PostgresOperator` with window functions in SQL | `dq_duplicate_detection` |

---

## How It Works

### Batch path (hourly, core pipeline)
1. Airflow `batch_etl_pipeline` wakes up on schedule
2. Extracts events from Postgres for the closed time window `[data_interval_start, data_interval_end)` — idempotent by design
3. Writes Parquet files to `/data/landing/YYYY/MM/DD/HH/`
4. Submits a PySpark job to the Spark cluster
5. Spark reads landing Parquet, aggregates by product, writes to `/data/gold/`

### Streaming path (continuous)
1. Events arrive in the `user-events` Kafka topic
2. Spark Structured Streaming (`streaming_consumer.py`) reads from Kafka in micro-batches
3. Writes aggregated metrics to `live_event_metrics` table in Postgres

### Dead Letter Queue
Any event that fails validation is routed to `user-events-dlq`. The `dlq_replay_pipeline` DAG periodically replays these — nothing is silently lost.

### Dynamic DAG Factory
Adding a new tenant requires only a JSON entry in `airflow/config/central_tenant_manifest.json` — no Python code changes:

```json
{
  "client_id": "newbrand",
  "schedule": "0 * * * *",
  "target_table": "analytics_newbrand",
  "retention_days": 90
}
```

---

## Useful Commands

```bash
# View all container statuses
docker compose ps

# Tail logs for a specific service
docker compose logs -f airflow-scheduler
docker compose logs -f kafka-broker
docker compose logs -f spark-master

# Trigger a DAG manually
docker exec airflow_scheduler airflow dags trigger batch_etl_pipeline

# List all DAGs and check for import errors
docker exec airflow_scheduler airflow dags list
docker exec airflow_scheduler airflow dags list-import-errors

# List Kafka topics
docker exec production_kafka kafka-topics \
  --list --bootstrap-server localhost:9092

# Connect to Postgres
docker exec -it production_postgres \
  psql -U engine_admin -d analytics_platform

# Full teardown (removes volumes/data)
docker compose down -v
```

---

## CI/CD

GitHub Actions runs on every push to `main` and every pull request:

| Job | What it checks |
|---|---|
| `airflow` | Imports all 26 DAGs — zero broken DAGs required to pass |
| `spark` | ruff lint on all PySpark jobs |

---

## Learning Path

This project was built phase by phase, focused on the data engineering layer:

| Phase | What was built |
|---|---|
| 1–2 | PostgreSQL schema, asyncpg connection pool, Kafka KRaft setup |
| 3–4 | Idempotent hourly ETL DAG, Parquet landing zone, Spark batch jobs |
| 5 | Dynamic DAG factory — config-driven, no code changes per tenant |
| 6 | Full Spark cluster (1 master + 2 workers), distributed PySpark jobs |
| 7 | Kafka integration, Spark Structured Streaming consumer |
| 8–9 | 24 additional DAGs across DQ, reporting, lifecycle, monitoring, orchestration |
| 10–11 | Multi-stage Dockerfiles, GitHub Actions CI/CD, DAG import validation |
