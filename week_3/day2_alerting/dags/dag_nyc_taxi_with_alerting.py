"""
NYC Taxi ETL pipeline with production alerting.

Demonstrates two complementary failure-detection layers:
  * ``on_failure_callback`` — hard failures (exceptions, timeouts)
  * ``sla`` + ``sla_miss_callback`` — soft failures (pipeline is slow, not broken)

A deliberately failing task is included behind a flag so the alerting path
can be exercised end to end.
"""

from __future__ import annotations
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator


## Preparing Alerts
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))
from alerts import alert_on_failure, alert_on_sla_miss


## Flip to True To Exercise the Failure-Alerting 
SIMULATE_FAILURE = True 
# Flip to True to Exercise the SLA-miss (adds an artificial delay)
SIMULATE_SLOW = False 


## Create Extract Function (dummy data)
def extract() -> int:
    print("Extracting trip records from source...")
    return 1000


## Create Transform Function  (untuk contoh masalah SLA)
def transform() -> None:
    if SIMULATE_SLOW:
        print("Simulating a slow transformation (SLA breach expected)...")
        time.sleep(900)
    print("Transforming trip records...")


## Create Load Function  (untuk contoh masalah failure callback)
def load() -> None:
    if SIMULATE_FAILURE:
        raise ConnectionError("could not connect to warehouse: connection refused")
    print("Loading trip records into the warehouse...")


## Define Args
default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": alert_on_failure,
}


## Create DAG
with DAG(
    dag_id="dag_nyc_taxi_with_alerting",
    description="NYC Taxi ETL with failure and SLA alerting",
    default_args=default_args,
    start_date=datetime(2026,7,1),
    schedule=None,
    catchup=False,
    sla_miss_callback=alert_on_sla_miss,
    tags=["alerting", "week3"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
        sla=timedelta(seconds=60),
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task 
