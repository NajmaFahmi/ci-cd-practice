import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


##### Create Dummy Data (CSV file, 3 days data)

np.random.seed(42)
BRANCHES = ["JKT-01", "JKT-02", "BDG-01", "SBY-01", "MDN-01"]
PRODUCTS = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset"]


def generate_day(date_str, n=5000):
    """Generate transaksi untuk satu hari."""
    base = datetime.strptime(date_str, "%Y-%m-%d")
    df = pd.DataFrame({
        "transaction_id": [f"TRX-{date_str}-{i:06d}" for i in range(n)],
        "branch_code": np.random.choice(BRANCHES, n),
        "product_name": np.random.choice(PRODUCTS, n),
        "quantity": np.random.randint(1, 6, n),
        "unit_price": np.random.choice([150000, 250000, 500000, 1200000, 3500000], n),
        "transaction_ts": [
            base + timedelta(seconds=int(s))
            for s in np.random.randint(0, 86400, n)
        ],
    })
    df["total_amount"] = df["quantity"] * df["unit_price"]
    return df

os.makedirs("local_data", exist_ok=True)

for date_str in ["2026-06-15", "2026-06-16", "2026-06-17"]:
    df = generate_day(date_str)
    path = f"local_data/transactions_{date_str}.csv"
    df.to_csv(path, index=False)
    print(f"Generated {path} — {len(df)} rows")