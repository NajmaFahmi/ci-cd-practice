from google.cloud import storage
import pandas as pd
import io
import os

### ~~~~ Convert Raw Data CSV into Parquet (column-based)


## Define Bucket & File Name
BUCKET_NAME = "retailco-raw-najma"
DATES = ["2026-06-15", "2026-06-16", "2026-06-17"]


## Get Bucket
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


## Convert CSV into Parquet
for date_str in DATES:
    year, month, day = date_str.split("-")

    ## 1. Read Raw Data (CSV)
    raw_path = f"raw/year={year}/month={month}/day={day}/transactions.csv"
    raw_blob = bucket.blob(raw_path)
    csv_bytes = raw_blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(csv_bytes))


    ## 2. Casting Data Type Explicitly
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"])
    df["quantity"] = df["quantity"].astype("int32")
    df["unit_price"] = df["unit_price"].astype("int64")
    df["total_amount"] = df["total_amount"].astype("int64")


    ## 3. Write Data as Parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)


    ## 4. Upload Parquet Data to GCS
    processed_path = f"processed/year={year}/month={month}/day={day}/transactions.parquet"
    processed_blob = bucket.blob(processed_path)
    processed_blob.upload_from_file(buffer, content_type="application/octet-stream")

    
    ## 5. Get Data Size
    raw_blob.reload()
    processed_blob.reload()
    
    raw_kb = raw_blob.size / 1024
    pq_kb = processed_blob.size / 1024
    ratio = raw_kb / pq_kb
    print(f"{date_str}: CSV {raw_kb:.1f} KB → Parquet {pq_kb:.1f} KB  ({ratio:.1f}x lebih kecil)")


print("\nProcessed layer ready.")