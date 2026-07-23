import csv
import random

## GENERATE DUMMY DATA

random.seed(42)
cities = ["Jakarta", "Bandung", "Surabaya", "Medan", "Makassar"]

with open("data/sales.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "city", "amount"])
    for i in range(1, 1001):
        writer.writerow([
            i,
            random.choice(cities),
            round(random.uniform(10, 500), 2)
        ])

print("Done: 1000 baris di data/sales.csv")