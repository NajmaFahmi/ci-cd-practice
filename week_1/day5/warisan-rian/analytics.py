import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# TODO: ganti password kalau setup di laptop lain
conn_str = "postgresql://rian:passwordku123@localhost:5432/retail_analytics"

def load_sales_data():
    engine = create_engine(conn_str)
    df = pd.read_csv("sales_raw.csv")
    print(f"Loaded {len(df)} rows dari CSV")
    df.to_sql("sales_raw", engine, if_exists="replace", index=False)
    return engine

def calculate_monthly_metrics(engine):
    query = """
    SELECT 
        DATE_TRUNC('month', tanggal::date) as bulan,
        kategori,
        COUNT(*) as jumlah_transaksi,
        SUM(harga * jumlah) as total_revenue,
        AVG(harga) as rata_rata_harga
    FROM sales_raw
    GROUP BY DATE_TRUNC('month', tanggal::date), kategori
    ORDER BY bulan, total_revenue DESC
    """
    metrics = pd.read_sql(query, engine)
    metrics.to_sql("monthly_metrics", engine, if_exists="replace", index=False)
    print(f"Monthly metrics dihitung: {len(metrics)} baris")
    return metrics

if __name__ == "__main__":
    print("=== Pipeline Analytics Rian ===")
    engine = load_sales_data()
    result = calculate_monthly_metrics(engine)
    print("\n=== HASIL ===")
    print(result.to_string(index=False))