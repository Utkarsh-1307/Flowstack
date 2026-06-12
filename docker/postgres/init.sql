-- Run once on first container startup via docker-entrypoint-initdb.d

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email       VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_events (
    id          BIGSERIAL PRIMARY KEY,
    user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    event_type  VARCHAR(50) NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Composite index for time-range queries scoped to event type (Airflow windows)
CREATE INDEX IF NOT EXISTS idx_events_type_created
    ON raw_events(event_type, created_at DESC);

-- GIN index for arbitrary JSONB path queries
CREATE INDEX IF NOT EXISTS idx_events_jsonb_path
    ON raw_events USING gin (payload);

-- Live metrics table written to by the Spark Streaming job
CREATE TABLE IF NOT EXISTS live_event_metrics (
    id           BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end   TIMESTAMP WITH TIME ZONE NOT NULL,
    event_type   VARCHAR(50) NOT NULL,
    event_count  BIGINT NOT NULL,
    recorded_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_live_metrics_window
    ON live_event_metrics(window_start DESC);

-- Airflow needs its own schema in the same DB
-- (Set via AIRFLOW__DATABASE__SQL_ALCHEMY_CONN in docker-compose)
