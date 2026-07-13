import pandas as pd
from pathlib import Path
import logging


### Buat logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


### Define data path
RAW_PATH = Path("/app/data/raw/sales.csv")
OUTPUT_PATH = Path("/app/data/processed/metrics.csv")



### ETL Pipeline
def extract(path: Path) -> pd.DataFrame:
    logger.info(f"Extract: baca data dari {path}")
    df = pd.read_csv(path)
    logger.info(f"Extract selesai: {len(df)} baris")
    return df 


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Transform: hitung metrics per kategori")
    df["total_penjualan"] = df["harga"] * df["jumlah"]
    metrics = df.groupby("kategori").agg(
        jumlah_transaksi = ("produk", "count"),
        total_unit_terjual = ("jumlah", "sum"),
        total_revenue = ("total_penjualan", "sum"),
        rata_rata_harga = ("harga", "mean")
    ).reset_index()
    metrics = metrics.sort_values("total_revenue", ascending=False)
    logger.info(f"Transform selesai: {len(metrics)} kategori")
    return metrics 


def load(df: pd.DataFrame, path: Path) -> None:
    logger.info(f"Load: simpan hasil ke {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Load selesai: {len(df)} baris tersimpan")


def main() -> None:
    logger.info("=== Pipeline RetailCo ETL dimulai ===")
    raw_data = extract(RAW_PATH)
    hasil_metrics = transform(raw_data)
    load(hasil_metrics, OUTPUT_PATH)
    logger.info("=== Pipeline selesai ===")
    print("\n=== HASIL METRICS ===")
    print(hasil_metrics.to_string(index=False))


if __name__ == "__main__":
    main()
