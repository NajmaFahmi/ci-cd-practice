"""
Simulates an external vendor dropping a data file at an unpredictable
time. Run this in a separate terminal after triggering the DAG.
"""

import csv 
import random 
import time
from datetime import datetime
from pathlib import Path


## Define Target Folder & File
LANDING_ZONE = Path.home() / "de_learning/bulan_3/week_3/day1_sensors/landing_zone"
TARGET_FILE = LANDING_ZONE / "taxi_ready.csv"


## Create Dummy Data Vendor
ROWS = [
    ["trip_id", "fare_amount", "pickup_datetime"],
    ["T001", "12.50", "2026-07-13 08:00:00"],
    ["T002", "8.75", "2026-07-13 08:15:00"],
    ["T003", "22.00", "2026-07-13 08:30:00"],
    ["T004", "5.50", "2026-07-13 08:45:00"],
    ["T005", "15.25", "2026-07-13 09:00:00"],
]

## Create a Fake Delay so Data Arrive Late in the Folder
delay = random.randint(45, 120)         # late 45 - 120 seconds
print(f"[{datetime.now():%H:%M:%S}] Vendor simulator started.")
print(f"[{datetime.now():%H:%M:%S}] File will land in {delay} seconds...")
time.sleep(delay)

LANDING_ZONE.mkdir(parents=True, exist_ok=True)
with open(TARGET_FILE, "w", newline="") as target_file:
    csv.writer(target_file).writerows(ROWS)

print(f"[{datetime.now():%H:%M:%S}] File dropped: {TARGET_FILE}")


