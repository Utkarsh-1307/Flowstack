# FlowStack — Real-Time ETL & Analytics Platform

A production-grade, end-to-end data engineering platform built from scratch. Covers real-time event ingestion, stream processing, distributed batch computation, workflow orchestration, and a live analytics dashboard.

---

## Architecture

```
[ React Dashboard ]
        │
        │ HTTP POST / WebSocket
        ▼
[ FastAPI Gateway ]
        │
        │ Produce Event
        ▼
[ Apache Kafka ]
        │
   ┌────┴────┐
   ▼         ▼
[ Spark     [ MinIO / Local
  Streaming]   Data Lake ]
   │              │
   ▼              ▼
[ Postgres   [ Airflow
  Live DB ]    Orchestrator ]
                  │
                  ▼
           [ Spark Batch Job ]
                  │
                  ▼
         [ Postgres Gold Layer ]
```

**Key design decision:** Airflow is never in the synchronous ingestion path. Kafka absorbs all incoming traffic; Airflow only orchestrates scheduled batch jobs.

---

## Tech Stack

| Layer | Technology |
|---|---|
| API Gateway | FastAPI + asyncpg + Pydantic v2 |
| Message Queue | Apache Kafka (KRaft, no Zookeeper) |
| Stream Processing | Apache Spark Structured Streaming |
| Batch Orchestration | Apache Airflow 2.9 |
| Distributed Compute | Apache Spark 3.5 (1 master + 2 workers) |
| Database | PostgreSQL 15 |
| Frontend | React 18 + TypeScript + Vite + Recharts |
| Infrastructure | Docker Compose + Nginx reverse proxy |
| CI/CD | GitHub Actions |

---

## Project Structure

```
FlowStack/
├── .github/workflows/ci.yml       # CI: lint, type-check, DAG validation, Docker build
├── backend/                       # FastAPI async gateway
│   └── app/
│       ├── core/                  # Config, DB connection pool
│       ├── models/                # SQLAlchemy ORM models
│       ├── schemas/               # Pydantic v2 validation
│       ├── services/              # Kafka producer, WebSocket manager
│       └── routers/               # API endpoints
├── airflow/
│   ├── dags/
│   │   ├── batch_etl_pipeline.py  # Idempotent hourly ETL
│   │   └── dynamic_dag_factory.py # Auto-generates DAGs from JSON config
│   ├── config/
│   │   └── central_tenant_manifest.json
│   └── plugins/alerts.py          # Slack/Discord failure hooks
├── spark/
│   ├── apps/
│   │   ├── metrics_aggregation.py # Batch: landing → gold
│   │   ├── streaming_consumer.py  # Streaming: Kafka → Postgres live metrics
│   │   └── tenant_job.py          # Multi-tenant batch processor
│   └── core/                      # Shared schemas + validation helpers
├── frontend/
│   └── src/
│       ├── components/            # StatCard, BarChart, PipelineTable
│       └── hooks/                 # useMetricsWebSocket, usePipelineStatus
├── docker/
│   ├── postgres/init.sql          # Schema, indexes (composite + GIN)
│   ├── nginx/nginx.conf           # Reverse proxy + WebSocket upgrade
│   └── spark-cluster/             # Spark tuning config
├── data/
│   ├── landing/                   # Raw Parquet dumps (immutable)
│   ├── bronze/                    # Validated records
│   └── gold/                      # Aggregated analytics tables
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

First run takes ~15 minutes (downloading Spark, Airflow, Kafka images). Subsequent starts take under 30 seconds.

### 3. Open the services

| Service | URL | Credentials |
|---|---|---|
| **React Dashboard** | http://localhost:8090 | — |
| **API Docs (Swagger)** | http://localhost:8001/docs | — |
| **Airflow UI** | http://localhost:8083 | `admin` / `admin` |
| **Spark Master UI** | http://localhost:8082 | — |
| **API Health** | http://localhost:8001/health | — |

---

## Using the Platform

### Create a user

```bash
curl -X POST http://localhost:8001/api/v1/users/ \
  -H "Content-Type: application/json" \
  -d '{"email": "demo@flowstack.com", "password": "password123"}'
```

Copy the `id` from the response.

### Send events

```bash
# Replace USER_ID with the id you just got
curl -X POST http://localhost:8001/api/v1/events/ingest \
  -H "Content-Type: application/json" \
  -d '{"userId":"USER_ID","eventType":"purchase","productId":101}'
```

Supported event types: `purchase`, `view`, `add_to_cart`, `checkout`, `refund`

Send a few events with different types and watch the dashboard at http://localhost:8090 update in real time via WebSocket.

### Trigger a batch pipeline

In Airflow (http://localhost:8083), click `batch_etl_pipeline_v1` → **▶ Trigger DAG**. This runs the full extract → Spark transform → gold layer write cycle.

---

## How It Works

### Real-time path (sub-second)
1. `POST /api/v1/events/ingest` — FastAPI validates the payload with Pydantic
2. Event is produced to the `user-events` Kafka topic (acks=all, gzip compressed)
3. FastAPI broadcasts a WebSocket notification to all connected dashboard clients
4. Dashboard updates instantly — no HTTP polling

### Batch path (hourly)
1. Airflow `batch_etl_pipeline_v1` wakes up on schedule
2. Extracts events from Postgres for the closed time window `[start, end)` — idempotent by design
3. Writes Parquet files to `/data/landing/YYYY/MM/DD/HH/`
4. Submits a PySpark job to the Spark cluster
5. Spark reads landing Parquet, aggregates by product, writes to `/data/gold/`

### Dead Letter Queue
Any event that fails Kafka validation is routed to `user-events-dlq` instead of being dropped. Nothing is silently lost.

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
docker compose logs -f api-gateway
docker compose logs -f kafka-broker
docker compose logs -f airflow-scheduler

# List Kafka topics
docker exec production_kafka kafka-topics \
  --list --bootstrap-server localhost:9092

# Connect to Postgres
docker exec -it production_postgres \
  psql -U engine_admin -d analytics_platform

# Restart a single service after a code change
docker compose restart api-gateway

# Full teardown (removes volumes/data)
docker compose down -v
```

---

## CI/CD

GitHub Actions runs on every push to `main` and every pull request:

| Job | What it checks |
|---|---|
| `backend` | ruff lint + pytest (against real Postgres) |
| `spark` | ruff lint on all PySpark jobs |
| `airflow` | DAG import validation (no broken DAGs) |
| `frontend` | TypeScript type-check + Vite production build |
| `docker-build` | Builds all 4 Docker images (runs after all above pass) |

---

## Learning Path

This project was built phase by phase:

| Phase | What was built |
|---|---|
| 1–2 | FastAPI gateway, PostgreSQL schema, asyncpg pool |
| 3–4 | Modular ETL scripts, idempotent Airflow DAGs |
| 5 | Dynamic DAG factory (config-driven, no code changes per tenant) |
| 6 | Spark cluster, distributed batch PySpark jobs |
| 7 | Kafka integration, Spark Structured Streaming |
| 8 | React dashboard, WebSocket live feed, Recharts |
| 9–11 | Multi-stage Dockerfiles, Nginx proxy, GitHub Actions CI/CD |
