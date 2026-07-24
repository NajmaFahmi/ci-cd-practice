from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when
import time



## CREATE SPARK
spark = SparkSession.builder \
    .appName("CachingDemo") \
    .master("local[4]") \
    .getOrCreate()

## Set log only when error occurs
spark.sparkContext.setLogLevel("ERROR")



## READ DATA
df = spark.read.csv("data/sales_dirty.csv", header=True, inferSchema=True)

## PREPROCESSING DATA
## Build an Expensive DataFrame
df_processed = df.filter(col("amount").isNotNull()) \
                .withColumn("city", upper(col("city"))) \
                .dropDuplicates(["order_id"]) \
                .withColumn("category",
                            when(col("amount") < 100, "small")
                            .when(col("amount") < 300, "medium")
                            .otherwise("large"))



## CACHE VS NOT CACHE
## WITHOUT CACHE
# Three actions, each recomputing from scratch.
print("=== WITHOUT CACHE ===")
start = time.time()
df_processed.count()
df_processed.groupBy("city").count().collect()
df_processed.groupBy("category").count().collect()
elapsed_no_cache = time.time() - start
print(f"Time without cache: {elapsed_no_cache:.2f}s")


## WITH CACHE
# The first action computes and stores to memory; the next two read from cache.
print("=== WITH CACHE ===")
df_processed.cache()
start = time.time()
df_processed.count()    # first action: compute + store in memory
df_processed.groupBy("city").count().collect()      # from cache
df_processed.groupBy("category").count().collect()  # from cache
elapsed_with_cache = time.time() - start
print(f"Time with cache: {elapsed_with_cache:.2f}s")
df_processed.unpersist()  # release memory cache

print(f"\n=== COMPARISON ===")
print(f"Without cache: {elapsed_no_cache:.2f}s")
print(f"With cache: {elapsed_with_cache:.2f}s")



## STOP SPARK
input("\nSpark UI at http://localhost:4040 — press Enter to stop.")
spark.stop()
