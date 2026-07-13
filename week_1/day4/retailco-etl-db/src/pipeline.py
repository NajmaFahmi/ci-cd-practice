import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
import os
import time
import logging


### Buat logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


### Connect dengan database (PostgreSQL)
DB_HOST = os.getenv("DB_HOST", "db")
DBT_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "najma")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rahasia")
DB_NAME = os.getenv("DB_NAME", "retailco")


### Define source data path
RAW_PATH = Path("/app/data/raw/sales.csv")


### Connect Database & ETL Pipeline
def wait_for_db(engine, max_retries: int=10) -> None:
    logger.info("Menunggu database siap...")
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database siap!")
            return
        except Exception:
            logger.info(f"Coba lagi ({i+1}/{max_retries})...")
            time.sleep(2)
    raise RuntimeError("Database tidak siap setelah 10x percobaan")


def extract(path: Path) -> pd.DataFrame:
    logger.info(f"Extract dari {path}")
    df = pd.read_csv(path)
    logger.info(f"Extract selesai: {len(df)} baris")
    return df 


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transform: hitung metrics per kategori")
    df["total_penjualan"] = df["harga"] * df["jumlah"]
    metrics = df.groupby("kategori").agg(
        jumlah_transaksi=("produk", "count"),
        total_unit_terjual=("jumlah", "sum"),
        total_revenue=("total_penjualan", "sum"),
        rata_rata_harga=("harga", "mean")
    ).reset_index()
    logger.info(f"Transform selesai: {len(metrics)} kategori")
    return metrics


def load(df: pd.DataFrame, engine) -> None:
    logger.info("Load ke PostgreSQL")
    df.to_sql("metrics_kategori", engine, if_exists="replace", index=False)
    logger.info(f"Load selesai: {len(df)} baris ke tabel metrics_kategori")


def main() -> None:
    logger.info("=== Pipeline ETL + PostgreSQL dimulai ===")
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DBT_PORT}/{DB_NAME}"
    engine = create_engine(url)
    wait_for_db(engine)
    data_raw = extract(RAW_PATH)
    hasil_metrics = transform(data_raw)
    load(hasil_metrics, engine)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM metrics_kategori ORDER BY total_revenue DESC"))
        print("\n=== HASIL DARI DATABASE ===")
        for row in result:
            print(row)
    logger.info("=== Pipeline selesai ===")


if __name__ == "__main__":
    main()
