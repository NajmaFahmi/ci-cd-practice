from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
import random

random.seed(42)

## CREATE SPARK
spark = SparkSession.builder \
    .appName("CachingDemo") \
    .master("local[4]") \
    .getOrCreate()

## Set log only when error occurs
spark.sparkContext.setLogLevel("ERROR")



## READ &  SETUP 2 DATA
## Sales Data (large data)
df_sales = spark.read.csv("data/sales_dirty.csv", header=True, inferSchema=True) \
                .filter(col("amount").isNotNull()) \
                .dropDuplicates(["order_id"])

## Customer Data (small data)
tiers = ["regular", "premium", "vip"]
customer_data = [(i, f"Customer_{i}", random.choice(tiers)) for i in range(1, 501)]
df_customers = spark.createDataFrame(customer_data, ["customer_id", "customer_name", "tier"])



## JOIN 2 DATAFRAMES
## By Default
# Spark chooses join strategy automatically
print("=== AUTOMATIC JOIN STRATEGY ===")
df_auto = df_sales.join(df_customers, "customer_id")
df_auto.explain()

## Force Explicit Broadcast
# Force broadcast with a hint
print("=== EXPLICIT BROADCAST ===")
df_broadcast = df_sales.join(broadcast(df_customers), "customer_id")
df_broadcast.explain()

## SortMergeJoin
## By disabling auto-broadcast, and forced to used SortMergeJoin
print("=== DISABLE AUTO-BROADCAST (force SortMergeJoin) ===")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
df_sortmerge = df_sales.join(df_customers, "customer_id")
df_sortmerge.explain()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)



## STOP SPARK
input("\nSpark UI at http://localhost:4040 — press Enter to stop.")
spark.stop()

