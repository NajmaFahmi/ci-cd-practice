import pandas as pd

data = {
    "nama": ["Najma", "Dani", "Citra"],
    "kota": ["Jakarta", "Bandung", "Surabaya"],
    "skor": [95, 87, 92]
}

df = pd.DataFrame(data)
print("=== Data Pipeline Output ===")
print(df.to_string(index=False))
print(f"\nRata-rata skor: {df['skor'].mean()}")
print(f"Skor tertinggi: {df.loc[df['skor'].idxmax(), 'nama']} ({df['skor'].max()})")