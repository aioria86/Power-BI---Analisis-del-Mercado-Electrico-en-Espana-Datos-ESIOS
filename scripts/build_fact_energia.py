import os
import glob
import pandas as pd


def load_data(indicador):

    files = glob.glob(f"data/raw/esios/{indicador}/*.parquet")

    if not files:
        print(f"⚠️ No hay archivos para {indicador}")
        return None

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    print(f"✔ {indicador} cargado ({len(df)} filas)")

    return df


def preparar_precio(df, nombre_columna):

    # asegurar datetime
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    # bajar a nivel horario
    df["datetime"] = df["datetime"].dt.floor("h")

    # agrupar
    df = df.groupby("datetime").agg({
        "value": "mean"
    }).reset_index()

    df.rename(columns={"value": nombre_columna}, inplace=True)

    return df


if __name__ == "__main__":

    print("🚀 Construyendo fact_energia...\n")

    # =========================
    # 🔹 DEMANDA (ya horaria)
    # =========================
    df_real = load_data("demanda_real")
    df_prev = load_data("demanda_prevista")
    df_prog = load_data("demanda_programada")

    df_real = df_real.rename(columns={"value": "demanda_real"})
    df_prev = df_prev.rename(columns={"value": "demanda_prevista"})
    df_prog = df_prog.rename(columns={"value": "demanda_programada"})

    # =========================
    # 🔹 PRECIOS (requieren transformación)
    # =========================
    df_precio = load_data("precio_mercado")
    df_ajustes = load_data("precio_ajustes")

    df_precio = preparar_precio(df_precio, "precio_mercado")
    df_ajustes = preparar_precio(df_ajustes, "precio_ajustes")

    # =========================
    # 🔗 MERGE
    # =========================
    df = df_real.merge(
        df_prev[["datetime", "demanda_prevista"]],
        on="datetime",
        how="left"
    )

    df = df.merge(
        df_prog[["datetime", "demanda_programada"]],
        on="datetime",
        how="left"
    )

    df = df.merge(
        df_precio,
        on="datetime",
        how="left"
    )

    df = df.merge(
        df_ajustes,
        on="datetime",
        how="left"
    )

    # =========================
    # 📊 VALIDACIONES
    # =========================
    print("\nValidaciones:")
    print(f"Filas totales: {len(df)}")

    print("\nValores nulos:")
    print(df[[
        "demanda_real",
        "demanda_prevista",
        "demanda_programada",
        "precio_mercado",
        "precio_ajustes"
    ]].isnull().sum())

    # =========================
    # 📁 GUARDAR
    # =========================
    os.makedirs("data/processed", exist_ok=True)

    output = "data/processed/fact_energia.parquet"
    df.to_parquet(output, index=False)

    print(f"\n✅ Fact table guardada en: {output}")
    print("🎯 Proceso completado")