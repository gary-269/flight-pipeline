"""
Airflow DAG: end-to-end OpenSky flight pipeline.

    ingest_bronze -> transform_silver -> quality_checks -> build_gold

Scheduled every 10 minutes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow/scripts")

import ingest_bronze
import transform_silver
import build_gold
import quality_checks

default_args = {
    "owner": "karanveer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _ingest(**context):
    batch_id = ingest_bronze.run()
    context["ti"].xcom_push(key="batch_id", value=batch_id)


def _transform(**context):
    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="ingest_bronze")
    transform_silver.run(batch_id)


def _quality_check(**context):
    quality_checks.run()


def _build_gold(**context):
    build_gold.run()

with DAG(
    dag_id="opensky_flight_pipeline",
    description="Bronze/Silver/Gold pipeline for live OpenSky flight data over the GTA",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=["flights", "opensky", "medallion"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_ingest,
    )

    silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=_transform,
    )

    quality_task = PythonOperator(
        task_id="quality_checks",
        python_callable=_quality_check,
    )

    gold_task = PythonOperator(
        task_id="build_gold",
        python_callable=_build_gold,
    )

    ingest_task >> silver_task >> quality_task >> gold_task