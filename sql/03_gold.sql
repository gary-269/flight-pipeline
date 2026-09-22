CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_kpis (
    day                 DATE PRIMARY KEY,
    distinct_aircraft   INTEGER,
    total_snapshots     INTEGER,
    avg_velocity_ms     NUMERIC(8,2),
    avg_altitude_m      NUMERIC(8,2),
    pct_on_ground       NUMERIC(5,2),
    pct_missing_position NUMERIC(5,2),
    emergency_events    INTEGER,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.hourly_trends (
    hour_of_day     INTEGER PRIMARY KEY CHECK (hour_of_day BETWEEN 0 AND 23),
    avg_aircraft_count NUMERIC(8,2),
    avg_velocity_ms NUMERIC(8,2),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.top_aircraft (
    icao24          TEXT PRIMARY KEY,
    last_callsign   TEXT,
    origin_country  TEXT,
    appearances     INTEGER,
    first_seen      TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);