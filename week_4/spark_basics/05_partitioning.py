from pyspark.sql import SparkSession
from pyspark.sql.functions import col


## CREATE SPARK
spark = SparkSession.builder \
    .appName("PartitioningDemo") \
    .master("local[4]") \
    .getOrCreate()

## Set log only when error occurs
spark.sparkContext.setLogLevel("ERROR")



## READ & SETUP DATA
df = spark.read.csv("data/sales_dirty.csv", header=True, inferSchema=True)

## Check initial partition
print(f"Initial partitions: {df.rdd.getNumPartitions()}")



## CHANGE TOTAL PARTITION

## 1. REPARTITION (increase, with shuffle)
## The data is shuffled evenly into 8 partitions.
df_repartitioned = df.repartition(8)
print(f"After repartition(8): {df_repartitioned.rdd.getNumPartitions()}")


## 2. COAlESCE (decrease, no shuffle)
## The 8 partitions are merged into 2 without a shuffle.
df_coalesced = df_repartitioned.coalesce(2)
print(f"After coalesce(2): {df_coalesced.rdd.getNumPartitions()}")


## 3. SHUFFLE PARTITIONS (default = 200 partitions)
## By Default
spark.conf.set("spark.sql.shuffle.partitions", 200)
df_grouped_200 = df.groupBy("city").count()
df_grouped_200.count()
print(f"groupBy with shuffle.partitions=200: {df_grouped_200.rdd.getNumPartitions()} partitions")
# with the default setting (200), the groupBy result has 200 partitions, even though there are only 5 cities — 195 empty

## Change Partition Number
spark.conf.set("spark.sql.shuffle.partitions", 8)
df_grouped_8 = df.groupBy("city").count()
df_grouped_8.count()
print(f"groupBy with shuffle.partitions=8: {df_grouped_8.rdd.getNumPartitions()} partitions")
# with the setting at 8, only 8 partitions



## STOP SPARK
input("\nSpark UI at http://localhost:4040 — press Enter to stop.")
spark.stop()