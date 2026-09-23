"""
Monitoring & data quality checks. Runs between Silver and Gold.
Raises an exception on failure so Airflow marks the task failed,
which is what should trigger an alert.
"""

import os
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("quality_checks")

DB_DSN = (
    f"host={os.getenv('WAREHOUSE_HOST', 'localhost')} "
    f"port={os.getenv('WAREHOUSE_PORT', '5432')} "
    f"dbname={os.getenv('WAREHOUSE_DB', 'flights')} "
    f"user={os.getenv('WAREHOUSE_USER', 'flights_user')} "
    f"password={os.getenv('WAREHOUSE_PASSWORD', 'flights_pass')}"
)

MAX_NULL_POSITION_PCT = 90.0
ROW_COUNT_DEVIATION_PCT = 50.0

def check_latest_batch(cur):
    cur.execute(
        """
        SELECT batch_id, rows_in_bronze, rows_written_silver,
               pct_null_position, pct_null_velocity, emergency_count, passed, run_at
        FROM silver.dq_log
        ORDER BY run_at DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("No entries in silver.dq_log -- has Silver ever run?")

    batch_id, rows_in, rows_out, pct_null_pos, pct_null_vel, emergencies, passed, run_at = row
    log.info(
        "Latest batch %s (%s): in=%d out=%d null_pos=%.1f%% null_vel=%.1f%% emergencies=%d",
        batch_id, run_at, rows_in, rows_out, pct_null_pos, pct_null_vel, emergencies,
    )

    if rows_out == 0:
        raise RuntimeError(f"Batch {batch_id}: zero rows written to Silver.")

    if pct_null_pos > MAX_NULL_POSITION_PCT:
        raise RuntimeError(
            f"Batch {batch_id}: {pct_null_pos}% of rows missing position "
            f"(threshold {MAX_NULL_POSITION_PCT}%)."
        )

    if emergencies > 0:
        log.warning("Batch %s: %d emergency squawk(s) detected -- not a failure, just flagging.",
                     batch_id, emergencies)

    return True

def check_row_count_anomaly(cur):
    cur.execute(
        """
        WITH daily_counts AS (
            SELECT last_contact::date AS day, COUNT(*) AS n
            FROM silver.state_vectors
            WHERE last_contact >= now() - INTERVAL '8 days'
            GROUP BY last_contact::date
        )
        SELECT
            (SELECT n FROM daily_counts WHERE day = CURRENT_DATE) AS today_count,
            (SELECT AVG(n) FROM daily_counts WHERE day < CURRENT_DATE) AS trailing_avg
        """
    )
    today_count, trailing_avg = cur.fetchone()
    if trailing_avg is not None:
        trailing_avg = float(trailing_avg)

    if trailing_avg is None or today_count is None:
        log.info("Not enough history yet for row-count anomaly check -- skipping.")
        return True

    deviation_pct = 100.0 * abs(today_count - trailing_avg) / trailing_avg
    log.info(
        "Today's snapshot count: %d, 7-day avg: %.1f, deviation: %.1f%%",
        today_count, trailing_avg, deviation_pct,
    )

    if deviation_pct > ROW_COUNT_DEVIATION_PCT:
        raise RuntimeError(
            f"Row-count anomaly: today={today_count} vs 7-day avg={trailing_avg:.1f} "
            f"({deviation_pct:.1f}% deviation, threshold {ROW_COUNT_DEVIATION_PCT}%)."
        )

    return True

def run():
    conn = psycopg2.connect(DB_DSN)
    try:
        with conn.cursor() as cur:
            check_latest_batch(cur)
            check_row_count_anomaly(cur)
        log.info("All quality checks passed.")
    finally:
        conn.close()


if __name__ == "__main__":
    run()