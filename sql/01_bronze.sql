CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_states (
    id           BIGSERIAL PRIMARY KEY,
    batch_id     UUID NOT NULL,
    source       TEXT NOT NULL DEFAULT 'opensky_states_all',
    request_time TIMESTAMPTZ NOT NULL,
    api_time     BIGINT,
    payload      JSONB NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_states_ingested_at
    ON bronze.raw_states (ingested_at);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_states_batch
    ON bronze.raw_states (batch_id);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_states_payload_gin
    ON bronze.raw_states USING GIN (payload);