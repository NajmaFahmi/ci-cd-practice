from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, lower, round as spark_round
from pyspark.sql.functions import sum as spark_sum, avg, count, max, min
from pyspark.sql.functions import when, lit


### CREATE SPARK
spark = SparkSession.builder \
    .appName("BasicOperations") \
    .master("local[4]") \
    .getOrCreate()

## Show Log Only When Error Occurs
spark.sparkContext.setLogLevel("ERROR")

### READ DATA
df = spark.read.csv("data/sales_dirty.csv", header=True, inferSchema=True)


# ============================================================
# BLOK 1 — INSPEKSI AWAL
# Tujuan: kenali data sebelum diapa-apakan
# ============================================================

print("=== SCHEMA ===")
df.printSchema()

print("=== 5 BARIS PERTAMA ===")
df.show(5)
df.show(5, truncate=False)

print("=== STATISTIK DASAR ===")
df.describe().show()

print("=== JUMLAH BARIS ===")
print(f"Total: {df.count()}")       # pakai print

print("=== NAMA KOLOM ===")
print(df.columns)       # pakai print



# ============================================================
# BLOK 2 — SELECT
# Tujuan: pilih kolom yang diperlukan
# Ada 2 cara penulisan untuk setiap case — hasilnya sama
# ============================================================

# ============================================================
# Kapan output harus disimpan ke variable baru dan kapan tidak (hanya di print)?
# Tidak perlu disimpan: hanya untuk tampilkan sekali
# Perlu disimpan: DataFrame dipakai lebih dari sekali
# ============================================================

## CASE 1
# Cara paling pendek. Pakai kalau tidak perlu ekspresi.
print("\n=== SELECT: CARA 1 — string biasa ===")
# Tidak disimpan
df.select("order_id", "amount").show(5)
# Disimpan
df_2col = df.select("order_id", "amount")
df_2col.show(5)


## CASE 2
# Pakai col() kalau perlu ekspresi, kondisi, operasi, fungsi
print("=== SELECT: CARA 2 — pakai col() ===")
# Tidak disimpan
df.select(
    col("order_id"),
    spark_round(col("amount") * 1.11, 2).alias("amount_with_tax"),
    upper(col("city")).alias("city_upper")
).show(5)
# Disimpan
df_tax = df.select(
    col("order_id"),
    spark_round(col("amount") * 1.11, 2).alias("amount_with_tax"),
    upper(col("city")).alias("city_upper")
)
df_tax.show(5)



# ============================================================
# BLOK 3 — DROP
# Tujuan: buang kolom yang tidak diperlukan
# ============================================================

## Tidak disimpan
print("\n=== DROP ===")
df.drop("customer").show(5)
## Disimpan
df_no_cust = df.drop("customer")
df_no_cust.show(5)



# ============================================================
# BLOK 4 — WITHCOLUMN
# Tujuan: tambah kolom baru atau ubah kolom yang sudah ada --> UBAH KOLOM
# ============================================================

## CASE 1 -- Ubah kolom, nama sama
df_new = df.withColumn("city", lower(col("city")))
df_new.select("order_id", "city").show(5)


## CASE 2 -- Ubah kolom, nama beda --> sama dengan Tambah kolom
df_new = df_new.withColumn("city_upper", upper(col("city")))
df_new.select("order_id", "city", "city_upper").show(5)


## CASE 3 -- Tambah Kolom
## Tidak disimpan
df.withColumn("amount_idr", spark_round(col("amount") * 18000, 0)) \
    .select("order_id", "amount", "amount_idr") \
    .show(5)
## Disimpan
df_new = df.withColumn("amount_idr", spark_round(col("amount") * 18000, 0))
df_new.select("order_id", "amount", "amount_idr").show(5)



# ============================================================
# BLOK 5 — FILTER
# Tujuan: saring baris berdasarkan kondisi
# ============================================================

## CASE 1 -- filter 1 kondisi
## Tidak disimpan
print("\n=== FILTER: LANGSUNG COUNT TANPA VARIABEL ===")
print(f"Jumlah data dengan amount > 400: {df.filter(col('amount') > 400).count()} baris")
# atau
df_new.filter(col("city") == "jakarta").show(5)
## Disimpan
df_mahal = df.filter(col("amount") > 400)
df_mahal.show(5)


## CASE 2 -- chain dengan operasi lain
df.filter(col("amount") > 400) \
    .select("order_id", "city", "amount") \
    .orderBy(col("amount").desc()) \
    .show(5)


## CASE 3 -- filter multiple conditions
## AND --> &
df_new.filter(
    (col("city") == "jakarta") & (col("amount") > 200)
).show(5)

## OR --> |
df_new.filter(
    (col("city") == "jakarta") | (col("city") == "surabaya")
).count()
print(f"Total data Jakarta atau Surabaya: {df_new.filter((col('city')=='jakarta')|(col('city')=='surabaya')).count()} baris")

## ISIN --> .isin()
df_3cities = df_new.filter(col("city").isin(["jakarta", "bandung", "medan"]))
print(f"Total data Jakarta & Bandung & Surabaya: {df_3cities.count()} baris")

## NULLCHECK --> .isNull(), .isNotNull()
print(f"Total data dengan amount null: {df.filter(col('amount').isNull()).count()} baris")
print(f"Total data dengan amount tidak null: {df.filter(col('amount').isNotNull()).count()} baris")

## NEGASI --> ~
print(f"Total data selain Jakarta: {df.filter(~(col('city')=='Jakarta')).count()} baris")


## CASE 4 -- where identic with filter
print(f"Total data dengan amount > 400: {df.where(col('amount') > 400).count()} baris")



# ============================================================
# BLOK 6 — IMMUTABILITY: BUKTI EKSPLISIT
# Tujuan: buktikan secara jelas df asli tidak pernah berubah
# ============================================================

print("\n=== BUKTI IMMUTABILITY ===")
## SEBELUM
print(f"SEBELUM — df baris: {df.count()}, kolom: {df.columns}")
## OPERASI
df_modif = df.filter(col("amount") > 300) \
            .withColumn("city", upper(col("city"))) \
            .withColumn("pajak", col("amount") * 0.11) \
            .drop("customer_id") \
            .dropDuplicates(["order_id"])
## SESUDAH
print(f"df_modif baris: {df_modif.count()}, kolom: {df_modif.columns}")
print(f"SETELAH — df baris: {df.count()}, kolom: {df.columns}")
print("df asli tidak berubah sama sekali.")



# ============================================================
# BLOK 7 — GROUPBY + AGREGASI
# ============================================================

## CASE 1 -- groupby 1 column
print("\n=== GROUPBY: SINGGLE COLUMN ===")
## Tanpa disimpan
df_new.groupBy("city").count().show(10)
df_new.filter(col("amount").isNotNull()) \
    .groupBy("city").count().show()
## Disimpan
df_city = df_new.filter(col("amount").isNotNull()) \
            .groupBy("city").count()
df_city.show()


## CASE 2 -- groupby multiple columns
print("\n=== GROUPBY: MULTI COLUMNS ===")
df_new.filter(col("amount").isNotNull()) \
    .withColumn("city", upper("city")) \
    .withColumn("kategori",
                when(col("amount") < 100, "murah")
                .when(col("amount") < 300, "sedang")
                .otherwise("mahal")
                ) \
    .groupBy("city", "kategori") \
    .agg(count("order_id").alias("jumlah")) \
    .show(20)


## CASE 3 -- groupby multiple aggregation
print("=== GROUPBY: MULTI AGREGASI ===")
## Tanpa disimpan
df_new.filter(col("amount").isNotNull()) \
    .groupBy("city") \
    .agg(
        spark_round(spark_sum("amount"), 2).alias("total"),
        spark_round(avg("amount"), 2).alias("rata-rata"),
        spark_round(max("amount"), 2).alias("tertinggi"),
        spark_round(min("amount"), 2).alias("terendah"),
        count("order_id").alias("jumlah")
    ).show()



# ============================================================
# BLOK 8 — SORT
# ============================================================

print("\n=== SORT: ASCENDING (default) ===")
df.filter(col("amount").isNotNull()) \
  .select("order_id", "city", "amount") \
  .orderBy("amount") \
  .show(5)

print("=== SORT: DESCENDING ===")
df.filter(col("amount").isNotNull()) \
  .select("order_id", "city", "amount") \
  .orderBy(col("amount").desc()) \
  .show(5)


print("=== SORT: MULTI KOLOM ===")
df.filter(col("amount").isNotNull()) \
  .withColumn("city", upper(col("city"))) \
  .select("city", "amount") \
  .orderBy(col("city").asc(), col("amount").desc()) \
  .show(10)



# ============================================================
# BLOK 9 — DISTINCT
# ============================================================

## SHOW
df_new.select("city").distinct().show()

## COUNT
print(f"Nilai unik city: {df_new.select('city').distinct().count()}")
print(f"Kombinasi unik city+customer_id: {df_new.select('city', 'customer_id').distinct().count()}")



# ============================================================
# BLOK 10 — UNION
# ============================================================

## CASE 1
## Gabung dua dataframe per variable
print("\n=== UNION: GABUNG DUA DATAFRAME VERTIKAL ===")
df_jakarta = df_new.filter(col("city") == "jakarta")
print(f"Total data Jakarta: {df_jakarta.count()} baris")
df_bandung = df_new.filter(col("city") == "bandung")
print(f"Total data Bogor: {df_bandung.count()} baris")

df_union = df_jakarta.union(df_bandung)
print(f"Setelah union: {df_union.count()} baris")


## CASE 2
## Gabung dua dataframe tanpa variable
print("=== UNION: LANGSUNG TANPA VARIABEL ===")
print(f"Union langsung: {df_new.filter(col('city')=='jakarta').union(df_new.filter(col('city')=='bandung')).count()} baris")



## CLOSE SPARK
input("\nSpark UI di http://localhost:4040 — tekan Enter untuk stop.")
spark.stop()