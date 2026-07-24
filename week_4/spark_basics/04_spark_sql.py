from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, round as spark_round, sum as spark_sum


## CREATE SPARK
spark = SparkSession.builder \
    .appName("SparkSQLDemo") \
    .master("local[4]") \
    .getOrCreate()

## Set log only when error occurs
spark.sparkContext.setLogLevel("ERROR")



## READ & SETUP DATA
df = spark.read.csv("data/sales_dirty.csv", header=True, inferSchema=True)

## Preprocess Data
df_clean = df.filter(col("amount").isNotNull()) \
            .withColumn("city", upper(col("city")))

## Register as Temporary View for SQL, named 'sales'
df_clean.createOrReplaceTempView("sales")



## DATAFRAME API vs SQL
## Preprocessing using both methods
print("=== DATAFRAME API RESULT ===")
result_api = df_clean.filter(col("amount") > 100) \
                    .groupBy("city") \
                    .agg(spark_round(spark_sum("amount"), 2).alias("total_amount")) \
                    .orderBy(col("total_amount").desc())
result_api.show()
print("=== DATAFRAME API PHYSICAL PLAN ===")
result_api.explain()

print("=== SPARK SQL RESULT ===")
result_sql = spark.sql("""
    SELECT 
        city, 
        ROUND(sum(amount), 2) as total_amount
    FROM sales
    WHERE amount > 100
    GROUP BY city
    ORDER BY total_amount DESC
""")
result_sql.show()
print("=== SPARK SQL PHYSICAL PLAN ===")
result_sql.explain()



## STOP SPARK
input("\nSpark UI at http://localhost:4040 — press Enter to stop.")
spark.stop()