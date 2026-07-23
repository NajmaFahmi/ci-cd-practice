from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, sum as spark_sum, count, round as spark_round
import random



### SETUP SPARK
spark = SparkSession.builder \
    .appName("JoinExample") \
    .master("local[4]") \
    .getOrCreate()

## Show Log Only When Error Occurs
spark.sparkContext.setLogLevel("ERROR")



### READ SALES DATA
df_sales = spark.read.csv("data/sales_dirty.csv", header=True, inferSchema=True)
## Clean data
df_sales = df_sales.withColumn("city", upper(col("city"))) \
                    .filter(col("amount").isNotNull()) \
                    .dropDuplicates(["order_id"])

print("=== SALES (setelah cleaning) ===")
df_sales.show(5)
print(f"Total: {df_sales.count()} baris")



### CREATE CUSTOMER DATA
## Define value for the columns
columns = ["customer_id", "customer_name", "tier"]
tiers = ["regular", "premium", "vip"]
customer_data = [
    (i, f"Customer_{i}", random.choice(tiers)) for i in range(1, 501)
]

## Create data
df_customers = spark.createDataFrame(customer_data, columns)

print("\n=== CUSTOMERS ===")
df_customers.show(5)
print(f"Total: {df_customers.count()} baris")



### JOIN DATAFRAMES
## Inner join
df_joined = df_sales.join(df_customers, "customer_id", "inner")
print("\n=== INNER JOIN ===")
df_joined.select("order_id", "customer_id", "customer_name", "tier", "city", "amount").show(5)
print(f"Total setelah join: {df_joined.count()} baris")

## Aggregate join result
print("\n=== TOTAL AMOUNT PER TIER CUSTOMER ===")
df_joined.groupBy("tier").agg(
    spark_round(spark_sum("amount"), 2).alias("total_amount"),
    count("order_id").alias("total_order")
).orderBy(col("total_amount").desc()).show()



### EXECUTION PLAN
print("\n=== EXECUTION PLAN JOIN ===")
df_joined.explain()


### SHOW UI & STOP SPARK SESSION
input("\nSpark UI di http://localhost:4040 — tekan Enter untuk stop.")
spark.stop()