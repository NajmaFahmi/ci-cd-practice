import pandas as pd
import numpy as np
import os 


### ~~~~~~~~~ LATIHAN 1

## Create Dummy Data
np.random.seed(42)
n = 500_000

df = pd.DataFrame({
    "id": np.arange(n),
    "city": np.random.choice(["Jakarta", "Bandung", "Surabaya", "Medan"], n),
    "category": np.random.choice(["A", "B", "C"], n),
    "amount": np.random.randint(10,5000,n),
})


## Write Data in 2 Format
# CSV data
df.to_csv("data.csv", index=False)
# Parquet data
df.to_parquet("data.parquet", engine="pyarrow", compression="snappy")


## Get Data Size
csv_size = os.path.getsize("data.csv") / 1_000_000
pq_size = os.path.getsize("data.parquet") / 1_000_000

print(f"CSV     : {csv_size:.2f} MB")
print(f"Parquet : {pq_size:.2f} MB")
print(f"Rasio   : {csv_size / pq_size:.1f}x lebih kecil")


## Read Parquet Data 
# Full Data (all columns)
full = pd.read_parquet("data.parquet")
print("Semua kolom: ", full.shape)

# Only Selected Column (column pruning)
one = pd.read_parquet("data.parquet", columns=["amount"])
print("Satu kolom :", one.shape, "→ kolom lain tidak dibaca dari disk")


## Look at Internal Metadata
import pyarrow.parquet as pq_reader

meta = pq_reader.ParquetFile("data.parquet").metadata
print("Jumlah baris   :", meta.num_rows)
print("Jumlah kolom   :", meta.num_columns)
print("Jumlah row group:", meta.num_row_groups)
print("Schema         :\n", pq_reader.ParquetFile("data.parquet").schema)



### ~~~~~~~~~ LATIHAN 2
for codec in ["snappy", "gzip", "zstd"]:
    df.to_parquet(f"data_{codec}.parquet", engine="pyarrow", compression=codec)
    size = os.path.getsize(f"data_{codec}.parquet") / 1_000_000
    print(f"{codec:8} : {size:.2f} MB")