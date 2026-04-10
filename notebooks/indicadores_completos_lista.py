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

    # Solar (dataset antiguo que decidiste usar)
    "gen_solar": 1001,

    # Renovables principales
    "gen_eolica": 551,
    "gen_hidraulica": 546,

    # No renovables
    "gen_nuclear": 549,
    "gen_ciclo_combinado": 550,

    # Carbón
    "gen_hulla_antracita": 42,
    "gen_hulla_subbituminosa": 43,

    # Fuel y gas
    "gen_fuel": 45,
    "gen_gas": 46,

    # =========================
    # POTENCIA
    # =========================

    # Total sistema
    "pot_indisponible": 463,

    # DISPONIBLE
    "pot_disp_hidraulica": 472,
    "pot_disp_eolica": 473,
    "pot_disp_nuclear": 474,
    "pot_disp_carbon": 475,
    "pot_disp_solar": 476,
    "pot_disp_ciclo_combinado": 477,
    "pot_disp_fuel": 478,
    "pot_disp_gas": 479,

    # INSTALADA
    "pot_inst_hidraulica": 464,
    "pot_inst_eolica": 465,
    "pot_inst_nuclear": 466,
    "pot_inst_carbon": 467,
    "pot_inst_ciclo_combinado": 468,
    "pot_inst_fuel": 469,
    "pot_inst_gas": 470,
    "pot_inst_solar": 471,

   
    # histórico
    "bombeo_turbinacion_medida": 1152,
    "bombeo_consumo_medida": 1172,

    # tiempo real (reciente)
    
     "bombeo_turbinacion_medida": 1152,
    "bombeo_consumo_medida": 1172,

    "francia_import": 556,
    "francia_export": 560,
    "portugal_import": 557,
    "portugal_export": 561,
    "marruecos_import": 559,
    "marruecos_export": 563,
    "andorra_import": 558,
    "andorra_export": 562,

    "consumo_mercado_libre": 365,
    "consumo_mercado_regulado": 366,
    "consumo_directo_mercado": 367,
    "consumo_servicios_auxiliares": 368,

    # =========================
    # 🟩 MERCADOS (BASE)
    # =========================
    # ⚠️ Estos NO existen directos → se construyen
    # "mercado_diario": None,
    # "mercado_intradiario": None,


    # =========================
    # 🟨 RESTRICCIONES PBF
    # =========================
    "restricciones_pbf_subir": 1790,
    "restricciones_pbf_bajar": 1791,
    # puedes añadir más variantes si quieres mayor precisión
    # "restricciones_pbf_subir_sca": XXXX,
    # "restricciones_pbf_bajar_sca": XXXX,


    # =========================
    # 🟥 RESTRICCIONES TIEMPO REAL
    # =========================
    "restricciones_tr": 10052,


    # =========================
    # 🟪 BALANCE DEL SISTEMA
    # =========================
    "balance_rr": 10054,          # Energías de balance RR
    "regulacion_terciaria": 10055, # mFRR


    # =========================
    # 🟧 REGULACIÓN SECUNDARIA
    # =========================
    # ⚠️ Este es mixto → incluye varias cosas
    "regulacion_secundaria": 10323,


    # =========================
    # 🟦 EXTRA (RECOMENDADOS PARA ESCALAR)
    # =========================
    # Estos no son obligatorios para el donut, pero te sirven
    # para enriquecer el modelo y futuros dashboards

    # Costes de balance
    "gastos_balance": 1037,
    "ingresos_balance": 1038,

        # RESTRICCIONES (sumar luego)
    "restricciones_tr_subir": 10340,
    "restricciones_tr_bajar": 10341,

    # INTRADIARIO (opcional, avanzado)
    "intradiario_sesion_5": 616,
    "intradiario_sesion_6": 617,
    "intradiario_sesion_7": 618,

    # =========================
    # 🔴 DESVÍOS (TIEMPO REAL)
    # =========================
    "desvios_subir": 763,
    "desvios_bajar": 764,

    # =========================
    # 🔵 BALANCE DEL SISTEMA
    # =========================
    "energia_balance": 762,

}