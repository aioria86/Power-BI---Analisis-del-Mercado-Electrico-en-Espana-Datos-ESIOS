import os
import pandas as pd

BASE_PATH = "data/raw/esios"


def cargar_datos(indicador):

    carpeta = f"{BASE_PATH}/{indicador}"

    if not os.path.exists(carpeta):
        print(f"⚠️ Carpeta no existe: {indicador}")
        return None

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

    df = df.sort_values("datetime")

    return df


if __name__ == "__main__":

    print("Construyendo fact_energia_full_v2...\n")

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
    # GENERACIÓN
    # =========================
    for ind, col in [
        ("generacion_solar", "gen_solar"),
        ("gen_real_eolica", "gen_eolica"),
        ("gen_real_nuclear", "gen_nuclear"),
        ("gen_real_hidraulica", "gen_hidraulica"),
        ("gen_real_ciclo_combinado", "gen_ciclo_combinado"),
        ("gen_hulla_antracita", "gen_hulla_antracita"),
        ("gen_hulla_subbituminosa", "gen_hulla_subbituminosa"),
        ("gen_fuel", "gen_fuel"),
        ("gen_gas", "gen_gas"),
    ]:
        df = cargar_datos(ind)
        if df is not None:
            df = procesar_generacion(df, col)
            df_base = df_base.merge(df, on="datetime_hora", how="left")

    # =========================
    # DEMANDA PROGRAMADA TOTAL
    # =========================
    programada_total = cargar_datos("demanda_programada_total")
    if programada_total is not None:
        programada_total = procesar_generacion(programada_total, "demanda_programada_total")
        df_base = df_base.merge(programada_total, on="datetime_hora", how="left")

    # =========================
    # POTENCIA
    # =========================

    pot_ind = cargar_datos("pot_indisponible")
    if pot_ind is not None:
        pot_ind = procesar_generacion(pot_ind, "pot_indisponible")
        df_base = df_base.merge(pot_ind, on="datetime_hora", how="left")

    # Disponible
    for ind, col in [
        ("pot_disp_nuclear", "pot_disp_nuclear"),
        ("pot_disp_carbon", "pot_disp_carbon"),
        ("pot_disp_fuel", "pot_disp_fuel"),
        ("pot_disp_gas", "pot_disp_gas"),
        ("pot_disp_eolica", "pot_disp_eolica"),
        ("pot_disp_solar", "pot_disp_solar"),
        ("pot_disp_hidraulica", "pot_disp_hidraulica"),
    ]:
        df = cargar_datos(ind)
        if df is not None:
            df = procesar_generacion(df, col)
            df_base = df_base.merge(df, on="datetime_hora", how="left")

    # Instalada
    for ind, col in [
        ("pot_inst_nuclear", "pot_inst_nuclear"),
        ("pot_inst_carbon", "pot_inst_carbon"),
        ("pot_inst_ciclo_combinado", "pot_inst_ciclo_combinado"),
        ("pot_inst_gas", "pot_inst_gas"),
        ("pot_inst_eolica", "pot_inst_eolica"),
        ("pot_inst_solar", "pot_inst_solar"),
        ("pot_inst_hidraulica", "pot_inst_hidraulica"),
    ]:
        df = cargar_datos(ind)
        if df is not None:
            df = procesar_generacion(df, col)
            df_base = df_base.merge(df, on="datetime_hora", how="left")

    # =========================
    # ALMACENAMIENTO
    # =========================
    for ind, col in [
        ("bombeo_turbinacion_medida", "bombeo_turbinacion"),
        ("bombeo_consumo_medida", "bombeo_consumo"),
    ]:
        df = cargar_datos(ind)
        if df is not None:
            df = procesar_generacion(df, col)
            df_base = df_base.merge(df, on="datetime_hora", how="left")

    # =========================
    # INTERCONEXIONES
    # =========================
    for ind, col in [
        ("francia_import", "francia_import"),
        ("francia_export", "francia_export"),
        ("portugal_import", "portugal_import"),
        ("portugal_export", "portugal_export"),
        ("marruecos_import", "marruecos_import"),
        ("marruecos_export", "marruecos_export"),
    ]:
        df = cargar_datos(ind)
        if df is not None:
            df = procesar_generacion(df, col)
            df_base = df_base.merge(df, on="datetime_hora", how="left")

    # =========================
    # CONSUMO NUEVO
    # =========================
    for ind, col in [
        ("consumo_mercado_libre", "consumo_mercado_libre"),
        ("consumo_mercado_regulado", "consumo_mercado_regulado"),
        ("consumo_directo_mercado", "consumo_directo_mercado"),
        ("consumo_servicios_auxiliares", "consumo_servicios_auxiliares"),
    ]:
        df = cargar_datos(ind)
        if df is not None:
            df = procesar_generacion(df, col)
            df_base = df_base.merge(df, on="datetime_hora", how="left")

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
    output_path = "data/processed/fact_energia_full_v2.parquet"

    df_base.to_parquet(output_path, index=False)

    print(f"\n✅ Fact table guardada en: {output_path}")
    print("🎯 Proceso completado")