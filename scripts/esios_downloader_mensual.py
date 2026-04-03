import sys
import os

# Permitir imports desde raíz del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
import pandas as pd
from calendar import monthrange

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


def descargar_por_meses(indicator_id, nombre, año):

    print(f"🔁 Descarga mensual: {nombre} ({año})")

    dfs = []

    for mes in range(1, 13):

        start_date = f"{año}-{mes:02d}-01"
        last_day = monthrange(año, mes)[1]
        end_date = f"{año}-{mes:02d}-{last_day}"

        print(f"   📆 {start_date} → {end_date}")

        df_mes = descargar_indicador(indicator_id, nombre, start_date, end_date)

        if df_mes is not None:
            dfs.append(df_mes)

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final = df_final.drop_duplicates()
        return df_final

    return None


def guardar_parquet(df, nombre, año):

    carpeta = f"data/raw/esios/{nombre}"
    os.makedirs(carpeta, exist_ok=True)

    path = f"{carpeta}/{nombre}_{año}.parquet"
    df.to_parquet(path, index=False)

    print(f"✅ Guardado: {path}")


if __name__ == "__main__":

    años = list(range(2015, 2026))  # Ajusta si quieres

    print("🚀 INICIO DESCARGA ESIOS (MODO MENSUAL)\n")

    for año in años:

        print(f"\n📅 Año: {año}")

        for nombre, indicador_id in INDICADORES.items():

            # 🔥 Evitar duplicados
            path = f"data/raw/esios/{nombre}/{nombre}_{año}.parquet"

            if os.path.exists(path):
                print(f"⏭️ Ya existe, se omite: {nombre} {año}")
                continue

            print(f"⬇️ Descargando (mensual): {nombre}")

            df = descargar_por_meses(indicador_id, nombre, año)

            if df is not None:
                print(f"✔ {nombre} OK ({len(df)} filas)")
                guardar_parquet(df, nombre, año)
            else:
                print(f"⚠️ Falló completamente: {nombre}")

    print("\n🎯 DESCARGA MENSUAL COMPLETADA")