import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
import os
from pathlib import Path


### Create Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


### Define URL Database
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "rian")
DB_PASSWORD = os.getenv("DB_PASSWORD", "passwordku123")
DB_NAME = os.getenv("DB_NAME", "retail_analytics")

url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


### Define Data Path
RAW_PATH = Path("/app/data/raw/sales_raw.csv")


### ELT PIPELINE
def load_sales_data():
    engine = create_engine(url)
    df = pd.read_csv(RAW_PATH)
    logger.info(f"Loaded {len(df)} rows dari CSV")
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
    logger.info(f"Monthly metrics dihitung: {len(metrics)} baris")
    return metrics

if __name__ == "__main__":
    print("=== Pipeline Analytics Rian ===")
    engine = load_sales_data()
    result = calculate_monthly_metrics(engine)
    print("\n=== HASIL ===")
    print(result.to_string(index=False))