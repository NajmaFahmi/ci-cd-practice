# Pipeline Analytics

ELT Pipeline untuk menghitung metrics bulanan berdasarkan kategori barang.


## Cara jalankan:

Butuh Docker terinstall.

Build image:
\`\`\`bash
docker build -t retail_analytics .
\`\`\`

Jalankan pipeline:
\`\`\`bash
docker run rretail_analytics
\`\`\`

## Input dan Output

- Input: data/raw/sales.csv
- Output: data/processed/metrics.csv