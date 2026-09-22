"""
Silver transform: read raw Bronze batches, parse OpenSky's positional
array format into typed columns, clean it up, and upsert into
silver.state_vectors.
"""

import os
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("transform_silver")

DB_DSN = (
    f"host={os.getenv('WAREHOUSE_HOST', 'localhost')} "
    f"port={os.getenv('WAREHOUSE_PORT', '5432')} "
    f"dbname={os.getenv('WAREHOUSE_DB', 'flights')} "
    f"user={os.getenv('WAREHOUSE_USER', 'flights_user')} "
    f"password={os.getenv('WAREHOUSE_PASSWORD', 'flights_pass')}"
)

EMERGENCY_SQUAWKS = {"7500", "7600", "7700"}

def _epoch_to_ts(epoch):
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _clean_row(raw: list, batch_id: str):
    """Turn one OpenSky positional array into a typed tuple for Silver, or None to skip."""
    if not raw or raw[0] is None or raw[4] is None:
        return None

    icao24 = raw[0].strip()
    callsign = (raw[1] or "").strip() or None
    origin_country = raw[2]
    time_position = _epoch_to_ts(raw[3])
    last_contact = _epoch_to_ts(raw[4])
    longitude = raw[5]
    latitude = raw[6]
    baro_altitude = raw[7]
    on_ground = bool(raw[8]) if raw[8] is not None else False
    velocity = raw[9] if len(raw) > 9 else None
    true_track = raw[10] if len(raw) > 10 else None
    vertical_rate = raw[11] if len(raw) > 11 else None
    geo_altitude = raw[13] if len(raw) > 13 else None
    squawk = raw[14] if len(raw) > 14 else None

    has_position = longitude is not None and latitude is not None
    is_emergency = squawk in EMERGENCY_SQUAWKS

    return (
        icao24, callsign, origin_country, time_position, last_contact,
        longitude, latitude, baro_altitude, on_ground, velocity,
        true_track, vertical_rate, geo_altitude, squawk,
        is_emergency, has_position, batch_id,
    )

def process_batch(conn, batch_id: str, payload_states: list) -> dict:
    cleaned = [_clean_row(r, batch_id) for r in (payload_states or [])]
    cleaned = [r for r in cleaned if r is not None]

    n_in = len(payload_states or [])
    n_out = len(cleaned)
    n_missing_pos = sum(1 for r in cleaned if not r[15])
    n_missing_vel = sum(1 for r in cleaned if r[9] is None)
    n_emergency = sum(1 for r in cleaned if r[14])

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO silver.state_vectors (
                icao24, callsign, origin_country, time_position, last_contact,
                longitude, latitude, baro_altitude_m, on_ground, velocity_ms,
                true_track_deg, vertical_rate, geo_altitude_m, squawk,
                is_emergency, has_position, batch_id
            ) VALUES %s
            ON CONFLICT (icao24, last_contact) DO UPDATE SET
                callsign = EXCLUDED.callsign,
                origin_country = EXCLUDED.origin_country,
                time_position = EXCLUDED.time_position,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                baro_altitude_m = EXCLUDED.baro_altitude_m,
                on_ground = EXCLUDED.on_ground,
                velocity_ms = EXCLUDED.velocity_ms,
                true_track_deg = EXCLUDED.true_track_deg,
                vertical_rate = EXCLUDED.vertical_rate,
                geo_altitude_m = EXCLUDED.geo_altitude_m,
                squawk = EXCLUDED.squawk,
                is_emergency = EXCLUDED.is_emergency,
                has_position = EXCLUDED.has_position,
                loaded_at = now()
            """,
            cleaned,
        )

        pct_missing_pos = round(100.0 * n_missing_pos / n_out, 2) if n_out else 0.0
        pct_missing_vel = round(100.0 * n_missing_vel / n_out, 2) if n_out else 0.0
        passed = n_out > 0 and pct_missing_pos < 90.0

        cur.execute(
            """
            INSERT INTO silver.dq_log
                (batch_id, rows_in_bronze, rows_written_silver,
                 pct_null_position, pct_null_velocity, emergency_count, passed)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (batch_id, n_in, n_out, pct_missing_pos, pct_missing_vel, n_emergency, passed),
        )

    return {
        "rows_in": n_in, "rows_out": n_out,
        "pct_missing_position": pct_missing_pos,
        "pct_missing_velocity": pct_missing_vel,
        "emergency_count": n_emergency,
        "passed": passed,
    }

def run(batch_id: str = None):
    """
    Process a specific batch_id if given (what Airflow will pass in);
    otherwise process every Bronze batch that hasn't been logged in
    silver.dq_log yet -- safe to run standalone or as a backfill.
    """
    conn = psycopg2.connect(DB_DSN)
    try:
        with conn.cursor() as cur:
            if batch_id:
                cur.execute(
                    "SELECT batch_id, payload FROM bronze.raw_states WHERE batch_id = %s",
                    (batch_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT b.batch_id, b.payload
                    FROM bronze.raw_states b
                    LEFT JOIN silver.dq_log d ON d.batch_id = b.batch_id
                    WHERE d.batch_id IS NULL
                    ORDER BY b.ingested_at
                    """
                )
            rows = cur.fetchall()

        if not rows:
            log.info("No unprocessed Bronze batches found.")
            return

        for bid, payload in rows:
            bid_str = str(bid)
            states = payload.get("states") or []
            result = process_batch(conn, bid_str, states)
            conn.commit()
            log.info("Batch %s -> %s", bid_str, result)
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else None)