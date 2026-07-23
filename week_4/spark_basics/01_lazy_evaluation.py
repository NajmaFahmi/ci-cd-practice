from pyspark.sql import SparkSession


## Buat SparkSession (pintu masuk ke Spark)
spark = SparkSession.builder \
    .appName("LazyEvaluationDemo") \
    .master("local[4]") \
    .getOrCreate()


## Cetak log hanya jika ada ERROR
spark.sparkContext.setLogLevel("ERROR")


## Baca data CSV --> Baru dicatat, belum dieksekusi
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)


## Transformation --> Baru dicatat, belum dieksekusi
## filter nilai suatu kolom & ambil 2 kolom
print("=== TRANSFORMATION DICATAT, BELUM DIEKSEKUSI ===")
filtered = df.filter(df["amount"] > 100)
selected = filtered.select("city", "amount")
print("Sampai sini: belum ada data yang diproses.")


## Lihat execution plan 
print("\n=== EXECUTION PLAN ===")
selected.explain(True)


## ACTION 1; show() --> Langsung eksekusi
print("\n=== ACTION DIPANGGIL — BARU EXECUTOR JALAN ===")
selected.show(5)


## Buka Spark UI sebelum di stop
input("\nSpark UI aktif di http://localhost:4040 — tekan Enter untuk lanjut.")
spark.stop()