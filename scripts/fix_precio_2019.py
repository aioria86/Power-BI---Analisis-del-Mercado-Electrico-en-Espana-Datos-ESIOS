import requests
import pandas as pd
import os
from config.config import HEADERS, BASE_URL

indicator_id = 600
nombre = "precio_mercado"
año = 2019

dfs = []

for mes in range(1, 13):

    start_date = f"{año}-{mes:02d}-01"
    end_date = f"{año}-{mes:02d}-28"

    url = f"{BASE_URL}/{indicator_id}"

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "time_trunc": "hour"
    }

    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code != 200:
        print(f"❌ Error mes {mes}")
        continue

    data = response.json()
    valores = data["indicator"]["values"]

    df = pd.DataFrame(valores)

    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    dfs.append(df)

    print(f"✔ Mes {mes} OK ({len(df)} filas)")

# 🔗 unir todo
df_final = pd.concat(dfs, ignore_index=True)

# 📁 guardar
os.makedirs(f"data/raw/esios/{nombre}", exist_ok=True)

path = f"data/raw/esios/{nombre}/{nombre}_{año}.parquet"
df_final.to_parquet(path, index=False)

print(f"\n✅ Guardado completo 2019 en: {path}")