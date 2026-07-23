import csv 
import random
from datetime import datetime, timedelta

random.seed(42)

## Buat variasi untuk kolom 'city'
cities_variants = [
    "Jakarta", "jakarta", "JAKARTA",
    "Bandung", "bandung",
    "Surabaya", "SURABAYA",
    "Medan",
    "Makassar", "makassar"
]

## Define kolom 'start_date'
start_date = datetime(2026, 1, 1)


## Read CSV & Create Dummy Data
with open("data/sales_dirty.csv", "w", newline="") as file_data:
    writer = csv.writer(file_data)
    
    ## define columns
    writer.writerow(["order_id", "customer_id", "city", "amount", "order_date"])

    ## write data in rows (10.000 rows)
    for i in range(1, 10001):
        # data for 'amount' column
        amount = None if random.random() < 0.05 else round(random.uniform(10, 500), 2)
        # data for 'order_date' column
        random_days = random.randint(0, 180)
        order_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
        # write to rows
        writer.writerow([
            i,      # order_id
            random.randint(1,500),   # customer_id
            random.choice(cities_variants),     # city
            amount if amount is not None else "",   # amount
            order_date      # order_date
        ])
    
    ## add 50 new data
    for i in range(1, 51):
        writer.writerow([
            i,
            random.randint(1, 500),
            random.choice(cities_variants),
            round(random.uniform(10, 500), 2),
            (start_date + timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d")
        ])


print("Done: 10.050 baris di data/sales_dirty.csv (dengan null, variasi kota, duplicate)")
