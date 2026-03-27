import os
import glob
import pandas as pd


# Cargar todos los parquet de un indicador
def load_data(indicador):

    path = f"data/raw/esios/{indicador}/*.parquet"
    files = glob.glob(path)

    if not files:
        print(f"No se encontraron archivos para {indicador}")
        return None

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    print(f"✔ {indicador} cargado ({len(df)} filas)")

    return df


# MAIN
if __name__ == "__main__":

    print("Construyendo fact table de demanda...\n")

    # Cargar datos
    df_real = load_data("demanda_real")
    df_prevista = load_data("demanda_prevista")
    df_programada = load_data("demanda_programada")

    # Validación básica
    if df_real is None or df_prevista is None or df_programada is None:
        print("❌ Error cargando datos")
        exit()

    # Renombrar columnas
    df_real = df_real.rename(columns={"value": "demanda_real"})
    df_prevista = df_prevista.rename(columns={"value": "demanda_prevista"})
    df_programada = df_programada.rename(columns={"value": "demanda_programada"})

    # Merge principal
    df = df_real.merge(
        df_prevista[["datetime", "demanda_prevista"]],
        on="datetime",
        how="left"
    )

    df = df.merge(
        df_programada[["datetime", "demanda_programada"]],
        on="datetime",
        how="left"
    )

    # Ordenar columnas
    df = df[
        [
            "datetime",
            "fecha",
            "hora",
            "año",
            "mes",
            "geo_id",
            "geo_name",
            "demanda_real",
            "demanda_prevista",
            "demanda_programada"
        ]
    ]

    # Validación rápida
    print("\n Validaciones:")

    print(f"Filas totales: {len(df)}")
    print(f"Nulos demanda_real: {df['demanda_real'].isnull().sum()}")
    print(f"Nulos demanda_prevista: {df['demanda_prevista'].isnull().sum()}")
    print(f"Nulos demanda_programada: {df['demanda_programada'].isnull().sum()}")

    # Crear carpeta processed
    os.makedirs("data/processed", exist_ok=True)

    # 💾 Guardar parquet final
    output_path = "data/processed/fact_demanda.parquet"
    df.to_parquet(output_path, index=False)

    print(f"\nFact table guardada en: {output_path}")
    print("Proceso completado")