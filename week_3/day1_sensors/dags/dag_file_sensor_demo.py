"""
FileSensor demonstration DAG.

Simulates a real-world dependency on an external vendor file that
arrives at an unpredictable time. The pipeline waits for the file
to land before processing it, rather than assuming a fixed arrival schedule.
"""

from datetime import datetime 
from pathlib import Path 

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor 


## Define Target Folder & File
LANDING_ZONE = Path.home() / "de_learning/bulan_3/week_3/day1_sensors/landing_zone"
TARGET_FILE = LANDING_ZONE / "taxi_ready.csv"


## Create Function to Detect & Process File
def process_file() -> None:
    """Read the landed file and report basic statistics."""
    with open(TARGET_FILE, "r") as target_file:
        lines = target_file.readlines()
    row_count = len(lines) - 1      #exclude header

    print(f"File detected at {TARGET_FILE}")
    print(f"Total data rows: {row_count}")
    print(f"Header: {lines[0].strip()}")


## Create DAG with SENSOR
with DAG(
    dag_id = "dag_file_sensor_demo",
    description = "Wait for vendor file to land before processing",
    start_date = datetime(2026,7,1),
    schedule = None,
    catchup = False,
    tags = ["sensors", "week3"]
) as dag:
    
    start = EmptyOperator(task_id="start")

    # check if file is ready
    wait_for_file = FileSensor(
        task_id = "wait_for_vendor_file",
        filepath = str(TARGET_FILE),
        poke_interval = 30,
        timeout = 600,
        mode = "poke",
    )

    # if the file is ready, lanjut ke step process file
    process = PythonOperator(
        task_id = "process_file",
        python_callable = process_file,
    )

    # define the order
    start >> wait_for_file >> process