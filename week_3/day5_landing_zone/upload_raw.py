from google.cloud import storage
import os


### ~~~~ Upload Raw Data into GCS

## Define Bucket GCS & File Name
BUCKET_NAME = "retailco-raw-najma"
DATES = ["2026-06-15", "2026-06-16", "2026-06-17"]


## Get Storage & Bucket
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


## Upload Raw Data
for date_str in DATES:
    ## define local path
    local_path = f"local_data/transactions_{date_str}.csv"

    ## create hive-style partitioning for file key
    year, month, day = date_str.split("-")
    blob_path = f"raw/year={year}/month={month}/day={day}/transactions.csv"

    ## upload raw data
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

    ## check data size
    size_kb = os.path.getsize(local_path) / 1024
    print(f"Uploaded → gs://{BUCKET_NAME}/{blob_path}  ({size_kb:.1f} KB)")

print("\nRaw layer landing complete.")