from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, sum as spark_sum, avg, count, when, round as spark_round



### SETUP SPARK
spark = SparkSession.builder \
    .appName("DataFrameOperations") \
    .master("local[04]") \
    .getOrCreate()

## Show Log Only When Error Occurs
spark.sparkContext.setLogLevel("ERROR")



### READ & INSPECT DATA
df = spark.read.csv("data/sales_dirty.csv", header=True, inferSchema=True)

print("=== SCHEMA ===")
df.printSchema()

print("=== 5 BARIS PERTAMA ===")
df.show(5)

print("=== JUMLAH BARIS TOTAL ===")
print(f"Total: {df.count()} baris")



### CHECK DIRTY DATA
## Show null data in the 'amount' column
print("=== CEK NULL DI KOLOM AMOUNT ===")
df.filter(col("amount").isNull()).show(5)
print(f"Jumlah baris dengan amount null: {df.filter(col('amount').isNull()).count()}")

## Show distinct value for the 'city' column
print("=== VARIASI NAMA KOTA ===")
df.select("city").distinct().show()



### CLEANING DATA
## Standardize value in the 'city' column
df_clean = df.withColumn("city", upper(col("city")))
print("=== KOTA SETELAH STANDARDISASI ===")
df_clean.select("city").distinct().show()

## Handle null value in the 'amount' column (by removing null value)
df_clean = df_clean.filter(col("amount").isNotNull())
print(f"=== SETELAH BUANG NULL: {df_clean.count()} baris")

## Remove duplicate data in the 'order_id' column
df_clean = df_clean.dropDuplicates(["order_id"])
print(f"=== SETELAH BUANG DUPLICATE: {df_clean.count()} baris ===")



### FEATURE ENGINEERING
## Create new column: "amount_category"
df_clean = df_clean.withColumn(
    "amount_category",
    when(col("amount") < 100, "small")
    .when(col("amount") < 300, "medium")
    .otherwise("large")
)
print("=== KOLOM BARU amount_category ===")
df_clean.select("order_id", "amount", "amount_category").show(10)


### AGGREGATE
## Counting the total amount, average amount, and total order for each city
print("=== TOTAL PER KOTA ===")
df_clean.groupBy("city").agg(
    spark_round(spark_sum("amount"), 2).alias("total_amount"),
    spark_round(avg("amount"), 2).alias("avg_amount"),
    count("order_id").alias("total_order")
).orderBy(col("total_amount").desc()).show()

## Counting the total amount and total order per city and amount category
print("=== TOTAL PER KOTA DAN KATEGORI ===")
df_clean.groupBy("city", "amount_category").agg(
    spark_round(spark_sum("amount"), 2).alias("total_amount"),
    count("order_id").alias("total_order")
).orderBy("city", "amount_category").show(20)



### SHOW UI & STOP SPARK SESSION
input("\nSpark UI di http://localhost:4040 — tekan Enter untuk stop.")
spark.stop()