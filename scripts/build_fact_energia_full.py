import os
import pandas as pd

BASE_PATH = "data/raw/esios"


def cargar_datos(indicador):

    carpeta = f"{BASE_PATH}/{indicador}"

    dfs = []

    for file in os.listdir(carpeta):
        if file.endswith(".parquet"):
            df = pd.read_parquet(os.path.join(carpeta, file))
            dfs.append(df)

    if not dfs:
        print(f"⚠️ Sin datos: {indicador}")
        return None

    df_total = pd.concat(dfs, ignore_index=True)

    print(f"✔ {indicador} cargado ({len(df_total)} filas)")

    return df_total


def procesar_generacion(df, nombre_col):

    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    df["datetime_hora"] = df["datetime"].dt.floor("h")

    df_grouped = (
        df.groupby("datetime_hora")["value"]
        .sum()
        .reset_index()
        .rename(columns={"value": nombre_col})
    )

    return df_grouped


def procesar_base(df):

    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    df = df.copy()

    df = df.sort_values("datetime")

    return df


if __name__ == "__main__":

    print("🚀 Construyendo fact_energia_full...\n")

    # =========================
    # DEMANDA
    # =========================
    demanda_real = cargar_datos("demanda_real")
    demanda_prevista = cargar_datos("demanda_prevista")
    demanda_programada = cargar_datos("demanda_programada")

    df_base = procesar_base(demanda_real)

    df_base = df_base.rename(columns={"value": "demanda_real"})

    df_base = df_base.merge(
        demanda_prevista[["datetime", "value"]].rename(columns={"value": "demanda_prevista"}),
        on="datetime",
        how="left"
    )

    df_base = df_base.merge(
        demanda_programada[["datetime", "value"]].rename(columns={"value": "demanda_programada"}),
        on="datetime",
        how="left"
    )

    # =========================
    # PRECIOS
    # =========================
    precio_mercado = cargar_datos("precio_mercado")
    precio_ajustes = cargar_datos("precio_ajustes")

    precio_mercado = procesar_generacion(precio_mercado, "precio_mercado")
    precio_ajustes = procesar_generacion(precio_ajustes, "precio_ajustes")

    df_base["datetime_hora"] = df_base["datetime"].dt.floor("h")

    df_base = df_base.merge(precio_mercado, on="datetime_hora", how="left")
    df_base = df_base.merge(precio_ajustes, on="datetime_hora", how="left")

    # =========================
    # GENERACIÓN (desde 2021)
    # =========================
    generacion_solar = cargar_datos("generacion_solar")
    generacion_eolica = cargar_datos("generacion_eolica")
    generacion_hidraulica = cargar_datos("generacion_hidraulica")
    generacion_nuclear = cargar_datos("generacion_nuclear")
    generacion_ciclo = cargar_datos("generacion_ciclo_combinado")

    if generacion_solar is not None:
        solar = procesar_generacion(generacion_solar, "gen_solar")
        df_base = df_base.merge(solar, on="datetime_hora", how="left")

    if generacion_eolica is not None:
        eolica = procesar_generacion(generacion_eolica, "gen_eolica")
        df_base = df_base.merge(eolica, on="datetime_hora", how="left")

    if generacion_hidraulica is not None:
        hidraulica = procesar_generacion(generacion_hidraulica, "gen_hidraulica")
        df_base = df_base.merge(hidraulica, on="datetime_hora", how="left")

    if generacion_nuclear is not None:
        nuclear = procesar_generacion(generacion_nuclear, "gen_nuclear")
        df_base = df_base.merge(nuclear, on="datetime_hora", how="left")

    if generacion_ciclo is not None:
        ciclo = procesar_generacion(generacion_ciclo, "gen_ciclo_combinado")
        df_base = df_base.merge(ciclo, on="datetime_hora", how="left")

    # =========================
    # FEATURES TEMPORALES
    # =========================
    df_base["fecha"] = df_base["datetime"].dt.date
    df_base["hora"] = df_base["datetime"].dt.hour
    df_base["año"] = df_base["datetime"].dt.year
    df_base["mes"] = df_base["datetime"].dt.month

    # =========================
    # VALIDACIONES
    # =========================
    print("\nValidaciones:")

    print("Filas totales:", len(df_base))

    print("\nValores nulos:")
    print(df_base.isnull().sum())

    # =========================
    # GUARDAR
    # =========================
    output_path = "data/processed/fact_energia_full.parquet"

    df_base.to_parquet(output_path, index=False)

    print(f"\n✅ Fact table guardada en: {output_path}")
    print("🎯 Proceso completado")