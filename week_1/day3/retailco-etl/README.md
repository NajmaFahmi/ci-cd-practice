# RetailCo ETL Pipeline

Pipeline ETL sederhana untuk hitung metrics penjualan per kategori.

## Cara Menjalankan

Butuh Docker terinstall.

Build image:
\`\`\`bash
docker build -t retailco-etl .
\`\`\`

Jalankan pipeline:
\`\`\`bash
docker run retailco-etl
\`\`\`

## Input dan Output

- Input: data/raw/sales.csv
- Output: data/processed/metrics.csv