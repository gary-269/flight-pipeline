import os
import uuid
import json
import logging
from datetime import datetime, timezone

import requests
import psycopg2

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest_bronze")

OPENSKY_URL = "https://opensky-network.org/api/states/all"

BBOX = {
    "lamin": os.getenv("BBOX_LAMIN", "43.4"),
    "lomin": os.getenv("BBOX_LOMIN", "-80.2"),
    "lamax": os.getenv("BBOX_LAMAX", "44.0"),
    "lomax": os.getenv("BBOX_LOMAX", "-79.0"),
}

DB_DSN = (
    f"host={os.getenv('WAREHOUSE_HOST', 'localhost')} "
    f"port={os.getenv('WAREHOUSE_PORT', '5432')} "
    f"dbname={os.getenv('WAREHOUSE_DB', 'flights')} "
    f"user={os.getenv('WAREHOUSE_USER', 'flights_user')} "
    f"password={os.getenv('WAREHOUSE_PASSWORD', 'flights_pass')}"
)

OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)

def get_access_token() -> str:
    """
    Trade our client_id/client_secret for a short-lived bearer token.
    OpenSky's login server issues tokens that expire in about an hour,
    so we simply fetch a fresh one on every run rather than trying to
    cache and reuse it.
    """
    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        log.warning("No OpenSky credentials found -- falling back to anonymous access.")
        return None

    log.info("Requesting OpenSky access token...")
    resp = requests.post(
        OPENSKY_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": OPENSKY_CLIENT_ID,
            "client_secret": OPENSKY_CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Token endpoint responded but no access_token was returned.")
    log.info("Access token acquired.")
    return token


def fetch_states() -> dict:
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    log.info("Requesting OpenSky states for bbox %s", BBOX)
    resp = requests.get(OPENSKY_URL, params=BBOX, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    n_states = len(data.get("states") or [])
    log.info("Received %d aircraft state vectors", n_states)
    return data

def store_bronze(payload: dict, batch_id: str, request_time: datetime) -> None:
    conn = psycopg2.connect(DB_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze.raw_states
                    (batch_id, source, request_time, api_time, payload)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    "opensky_states_all",
                    request_time,
                    payload.get("time"),
                    json.dumps(payload),
                ),
            )
        log.info("Stored batch %s in bronze.raw_states", batch_id)
    finally:
        conn.close()


def run() -> str:
    """Fetch + store one snapshot. Returns the batch_id (useful for Airflow XCom)."""
    batch_id = str(uuid.uuid4())
    request_time = datetime.now(timezone.utc)
    payload = fetch_states()
    store_bronze(payload, batch_id, request_time)
    return batch_id


if __name__ == "__main__":
    run()