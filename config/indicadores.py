"""
Catálogo de indicadores ESIOS utilizados en el proyecto.

Este archivo centraliza todos los indicadores necesarios para:
- Descarga de datos desde la API ESIOS
- Construcción de tablas de hechos
- Análisis en Power BI

Los indicadores están organizados por bloques funcionales del sistema eléctrico.
"""

INDICADORES = {

    # =========================
    # DEMANDA
    # =========================
    "demanda_real": 1293,
    "demanda_prevista": 1294,
    "demanda_programada": 1295,
    "demanda_programada_total": 1296,

    # =========================
    # PRECIOS
    # =========================
    "precio_mercado": 600,
    "precio_ajustes": 612,

    # =========================
    # GENERACIÓN
    # =========================
    "generacion_solar": 1001,
    "gen_real_eolica": 551,
    "gen_real_hidraulica": 546,
    "gen_real_nuclear": 549,
    "gen_real_ciclo_combinado": 550,

    # Carbón
    "gen_hulla_antracita": 42,
    "gen_hulla_subbituminosa": 43,

    # Fuel y gas
    "gen_fuel": 45,
    "gen_gas": 46,

    # =========================
    # POTENCIA
    # =========================
    "pot_indisponible": 463,

    # Disponible
    "pot_disp_hidraulica": 472,
    "pot_disp_eolica": 473,
    "pot_disp_nuclear": 474,
    "pot_disp_carbon": 475,
    "pot_disp_solar": 476,
    "pot_disp_ciclo_combinado": 477,
    "pot_disp_fuel": 478,
    "pot_disp_gas": 479,

    # Instalada
    "pot_inst_hidraulica": 464,
    "pot_inst_eolica": 465,
    "pot_inst_nuclear": 466,
    "pot_inst_carbon": 467,
    "pot_inst_ciclo_combinado": 468,
    "pot_inst_fuel": 469,
    "pot_inst_gas": 470,
    "pot_inst_solar": 471,

    # =========================
    # ALMACENAMIENTO
    # =========================
    "bombeo_turbinacion_medida": 1152,
    "bombeo_consumo_medida": 1172,

    # =========================
    # INTERCONEXIONES
    # =========================
    "francia_import": 556,
    "francia_export": 560,
    "portugal_import": 557,
    "portugal_export": 561,
    "marruecos_import": 559,
    "marruecos_export": 563,
    "andorra_import": 558,
    "andorra_export": 562,

    # =========================
    # CONSUMO
    # =========================
    "consumo_mercado_libre": 365,
    "consumo_mercado_regulado": 366,
    "consumo_directo_mercado": 367,
    "consumo_servicios_auxiliares": 368,

    # =========================
    # SERVICIOS DE AJUSTE
    # =========================
    "restricciones_pbf_subir": 1790,
    "restricciones_pbf_bajar": 1791,
    "restricciones_tr": 10052,
    "balance_rr": 10054,
    "regulacion_terciaria": 10055,
    "regulacion_secundaria": 10323,

    # =========================
    # COSTES Y EXTRA
    # =========================
    "gastos_balance": 1037,
    "ingresos_balance": 1038,

    # Restricciones TR desglosadas
    "restricciones_tr_subir": 10340,
    "restricciones_tr_bajar": 10341,

    # Intradiario
    "intradiario_sesion_5": 616,
    "intradiario_sesion_6": 617,
    "intradiario_sesion_7": 618,

    # =========================
    # DESVÍOS Y BALANCE
    # =========================
    "desvios_subir": 763,
    "desvios_bajar": 764,
    "energia_balance": 762,

    # =========================
    # ENERGÍA NO INTEGRABLE (ERNI)
    # =========================
    "energia_no_integrable_total_pct": 10462,
    "energia_no_integrable_rtt_pct": 10460,
    "energia_no_integrable_rtd_pct": 10461,
    "energia_no_integrable_tiempo_real_pct": 10459,
}