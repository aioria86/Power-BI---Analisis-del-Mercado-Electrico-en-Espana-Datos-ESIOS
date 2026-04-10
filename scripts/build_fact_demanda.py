import os
import glob
import pandas as pd

BASE_PATH = "data/raw/esios"


def cargar_datos(indicador):

    # busca en subcarpeta
    path_sub = f"{BASE_PATH}/{indicador}/*.parquet"

    # compatibilidad con archivos planos
    path_flat = f"{BASE_PATH}/{indicador}_*.parquet"

    files = glob.glob(path_sub) + glob.glob(path_flat)

    if not files:
        print(f"⚠️ Sin datos: {indicador}")
        return None

    dfs = [pd.read_parquet(f) for f in files]

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


if __name__ == "__main__":

    print("Construyendo fact_energia_full_v2...\n")

    # =========================
    # BASE DESDE FACT DEMANDA
    # =========================
    df_base = pd.read_parquet("data/processed/fact_demanda.parquet")

    print(f"✔ fact_demanda cargado ({len(df_base)} filas)")

    # Crear datetime_hora
    df_base["datetime"] = pd.to_datetime(df_base["datetime"], utc=True)
    df_base["datetime_hora"] = df_base["datetime"].dt.floor("h")

    # =========================
    # PRECIOS
    # =========================
    precio_mercado = cargar_datos("precio_mercado")
    precio_ajustes = cargar_datos("precio_ajustes")

    if precio_mercado is not None:
        precio_mercado = procesar_generacion(precio_mercado, "precio_mercado")
        df_base = df_base.merge(precio_mercado, on="datetime_hora", how="left")

    if precio_ajustes is not None:
        precio_ajustes = procesar_generacion(precio_ajustes, "precio_ajustes")
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
    # CONSUMO
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
    # MERCADOS / AJUSTES
    # =========================
    for ind, col in [
        ("restricciones_pbf_subir", "restricciones_pbf_subir"),
        ("restricciones_pbf_bajar", "restricciones_pbf_bajar"),
        ("restricciones_tr", "restricciones_tr"),
        ("balance_rr", "balance_rr"),
        ("regulacion_terciaria", "regulacion_terciaria"),
        ("regulacion_secundaria", "regulacion_secundaria"),
        ("gastos_balance", "gastos_balance"),
        ("ingresos_balance", "ingresos_balance"),
    ]:
        df = cargar_datos(ind)
        if df is not None:
            df = procesar_generacion(df, col)
            df_base = df_base.merge(df, on="datetime_hora", how="left")

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