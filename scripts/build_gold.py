"""
Gold build: aggregate Silver into business-ready tables.
Reads ONLY from silver.state_vectors -- never touches Bronze directly.
"""

import os
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("build_gold")

DB_DSN = (
    f"host={os.getenv('WAREHOUSE_HOST', 'localhost')} "
    f"port={os.getenv('WAREHOUSE_PORT', '5432')} "
    f"dbname={os.getenv('WAREHOUSE_DB', 'flights')} "
    f"user={os.getenv('WAREHOUSE_USER', 'flights_user')} "
    f"password={os.getenv('WAREHOUSE_PASSWORD', 'flights_pass')}"
)

DAILY_KPIS_SQL = """
INSERT INTO gold.daily_kpis (
    day, distinct_aircraft, total_snapshots, avg_velocity_ms,
    avg_altitude_m, pct_on_ground, pct_missing_position, emergency_events
)
SELECT
    last_contact::date AS day,
    COUNT(DISTINCT icao24)               AS distinct_aircraft,
    COUNT(*)                             AS total_snapshots,
    ROUND(AVG(velocity_ms)::numeric, 2)  AS avg_velocity_ms,
    ROUND(AVG(baro_altitude_m)::numeric, 2) AS avg_altitude_m,
    ROUND(100.0 * SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_on_ground,
    ROUND(100.0 * SUM(CASE WHEN NOT has_position THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing_position,
    SUM(CASE WHEN is_emergency THEN 1 ELSE 0 END) AS emergency_events
FROM silver.state_vectors
GROUP BY last_contact::date
ON CONFLICT (day) DO UPDATE SET
    distinct_aircraft = EXCLUDED.distinct_aircraft,
    total_snapshots = EXCLUDED.total_snapshots,
    avg_velocity_ms = EXCLUDED.avg_velocity_ms,
    avg_altitude_m = EXCLUDED.avg_altitude_m,
    pct_on_ground = EXCLUDED.pct_on_ground,
    pct_missing_position = EXCLUDED.pct_missing_position,
    emergency_events = EXCLUDED.emergency_events,
    updated_at = now();
"""

HOURLY_TRENDS_SQL = """
INSERT INTO gold.hourly_trends (hour_of_day, avg_aircraft_count, avg_velocity_ms)
SELECT
    EXTRACT(HOUR FROM last_contact)::int AS hour_of_day,
    ROUND(COUNT(DISTINCT icao24)::numeric / GREATEST(COUNT(DISTINCT last_contact::date), 1), 2) AS avg_aircraft_count,
    ROUND(AVG(velocity_ms)::numeric, 2) AS avg_velocity_ms
FROM silver.state_vectors
GROUP BY EXTRACT(HOUR FROM last_contact)
ON CONFLICT (hour_of_day) DO UPDATE SET
    avg_aircraft_count = EXCLUDED.avg_aircraft_count,
    avg_velocity_ms = EXCLUDED.avg_velocity_ms,
    updated_at = now();
"""

TOP_AIRCRAFT_SQL = """
INSERT INTO gold.top_aircraft (icao24, last_callsign, origin_country, appearances, first_seen, last_seen)
SELECT
    icao24,
    (ARRAY_AGG(callsign ORDER BY last_contact DESC) FILTER (WHERE callsign IS NOT NULL))[1] AS last_callsign,
    (ARRAY_AGG(origin_country ORDER BY last_contact DESC))[1] AS origin_country,
    COUNT(*) AS appearances,
    MIN(last_contact) AS first_seen,
    MAX(last_contact) AS last_seen
FROM silver.state_vectors
GROUP BY icao24
ON CONFLICT (icao24) DO UPDATE SET
    last_callsign = EXCLUDED.last_callsign,
    origin_country = EXCLUDED.origin_country,
    appearances = EXCLUDED.appearances,
    first_seen = EXCLUDED.first_seen,
    last_seen = EXCLUDED.last_seen,
    updated_at = now();
"""

def run():
    conn = psycopg2.connect(DB_DSN)
    try:
        with conn, conn.cursor() as cur:
            log.info("Building gold.daily_kpis ...")
            cur.execute(DAILY_KPIS_SQL)
            log.info("Building gold.hourly_trends ...")
            cur.execute(HOURLY_TRENDS_SQL)
            log.info("Building gold.top_aircraft ...")
            cur.execute(TOP_AIRCRAFT_SQL)
        log.info("Gold layer rebuilt successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    run()