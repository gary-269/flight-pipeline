CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.state_vectors (
    icao24          TEXT NOT NULL,
    callsign        TEXT,
    origin_country  TEXT,
    time_position   TIMESTAMPTZ,
    last_contact    TIMESTAMPTZ NOT NULL,
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,
    baro_altitude_m DOUBLE PRECISION,
    on_ground       BOOLEAN NOT NULL DEFAULT FALSE,
    velocity_ms     DOUBLE PRECISION,
    true_track_deg  DOUBLE PRECISION,
    vertical_rate   DOUBLE PRECISION,
    geo_altitude_m  DOUBLE PRECISION,
    squawk          TEXT,
    is_emergency    BOOLEAN NOT NULL DEFAULT FALSE,
    has_position    BOOLEAN NOT NULL DEFAULT FALSE,
    batch_id        UUID NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (icao24, last_contact)
);

CREATE INDEX IF NOT EXISTS idx_silver_state_vectors_last_contact
    ON silver.state_vectors (last_contact);

CREATE INDEX IF NOT EXISTS idx_silver_state_vectors_icao24
    ON silver.state_vectors (icao24);

CREATE TABLE IF NOT EXISTS silver.dq_log (
    id                  BIGSERIAL PRIMARY KEY,
    batch_id            UUID NOT NULL,
    run_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_in_bronze      INTEGER,
    rows_written_silver INTEGER,
    pct_null_position   NUMERIC(5,2),
    pct_null_velocity   NUMERIC(5,2),
    emergency_count     INTEGER,
    passed              BOOLEAN
);