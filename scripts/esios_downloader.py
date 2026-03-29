import sys
import os

# Permitir imports desde raíz del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
import pandas as pd
from config.config import HEADERS, BASE_URL
from config.indicadores import INDICADORES


def descargar_indicador(indicator_id, nombre, start_date, end_date):

    url = f"{BASE_URL}/{indicator_id}"

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "time_trunc": "hour"
    }

    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code != 200:
        print(f"❌ Error en {nombre}: {response.status_code}")
        return None

    data = response.json()

    if "indicator" not in data or "values" not in data["indicator"]:
        print(f"⚠️ Estructura inesperada en {nombre}")
        return None

    valores = data["indicator"]["values"]

    if not valores:
        print(f"⚠️ Sin datos en {nombre}")
        return None

    df = pd.DataFrame(valores)

    columnas = [col for col in ["datetime", "value", "geo_id", "geo_name"] if col in df.columns]
    df = df[columnas]

    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    df["fecha"] = df["datetime"].dt.date
    df["hora"] = df["datetime"].dt.hour
    df["año"] = df["datetime"].dt.year
    df["mes"] = df["datetime"].dt.month

    df["indicador"] = nombre

    return df


def guardar_parquet(df, nombre, año):

    carpeta = f"data/raw/esios/{nombre}"
    os.makedirs(carpeta, exist_ok=True)

    path = f"{carpeta}/{nombre}_{año}.parquet"
    df.to_parquet(path, index=False)

    print(f"✅ Guardado: {path}")


if __name__ == "__main__":

    años = list(range(2015, 2026))  # 2015 → 2025

    print("🚀 INICIO DESCARGA ESIOS\n")

    for año in años:

        start_date = f"{año}-01-01"
        end_date = f"{año}-12-31"

        print(f"\n📅 Año: {año}")

        for nombre, indicador_id in INDICADORES.items():

            # 🔥 NUEVO: evitar descargas duplicadas
            path = f"data/raw/esios/{nombre}/{nombre}_{año}.parquet"

            if os.path.exists(path):
                print(f"⏭️ Ya existe, se omite: {nombre} {año}")
                continue

            print(f"⬇️ Descargando: {nombre}")

            df = descargar_indicador(indicador_id, nombre, start_date, end_date)

            if df is not None:
                print(f"✔ {nombre} OK ({len(df)} filas)")
                guardar_parquet(df, nombre, año)
            else:
                print(f"⚠️ Falló: {nombre}")

    print("\n🎯 DESCARGA COMPLETADA")