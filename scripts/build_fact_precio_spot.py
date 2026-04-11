import pandas as pd
import os
from glob import glob

print("Construyendo fact_precio_spot...")

# Ruta datos raw
path = "data/raw/esios/precio_mercado/*.parquet"

files = glob(path)

df_list = []

for file in files:
    df = pd.read_parquet(file)
    df_list.append(df)

df = pd.concat(df_list, ignore_index=True)

print(f"✔ precio_mercado cargado ({len(df)} filas)")

# =========================
# LIMPIEZA
# =========================

df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

df = df[["datetime", "geo_name", "value"]]

df = df.rename(columns={
    "geo_name": "pais",
    "value": "precio"
})

# =========================
# FILTRAR PAÍSES RELEVANTES
# =========================

paises = [
    "España",
    "Francia",
    "Portugal",
    "Alemania",
    "Bélgica",
    "Países Bajos"
]

df = df[df["pais"].isin(paises)]

# =========================
# FEATURES TIEMPO
# =========================

df["fecha"] = df["datetime"].dt.date
df["hora"] = df["datetime"].dt.hour
df["año"] = df["datetime"].dt.year
df["mes"] = df["datetime"].dt.month

# =========================
# GUARDADO
# =========================

output_path = "data/processed/fact_precio_spot.parquet"
df.to_parquet(output_path, index=False)

print(f"✅ Guardado en: {output_path}")
print("🎯 Proceso completado")