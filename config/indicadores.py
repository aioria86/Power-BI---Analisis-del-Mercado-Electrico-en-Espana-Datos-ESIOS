INDICADORES = {

    # DEMANDA
    "demanda_real": 544,
    "demanda_prevista": 545,
    "demanda_programada": 546,
    "demanda_programada_total": 1941,

    # PRECIOS (FASE 1)
    "precio_mercado": 600,
    "precio_ajustes": 612,

     # GENERACIÓN 
   # SOLAR (dataset antiguo)
    "gen_solar": 1001,

    # RESTO (T.Real)
    "gen_eolica": 551,
    "gen_nuclear": 549,
    "gen_hidraulica": 548,
    "gen_ciclo_combinado": 550,

    # GENERACIÓN NO LIMPIA
    
    # Carbón
    "gen_hulla_antracita": 42,
    "gen_hulla_subbituminosa": 43,

    # Fuel
    "gen_fuel": 45,

    # Gas (turbinas)
    "gen_gas": 46,


    # =========================
    # POTENCIA (MW)
    # =========================

    # INDISPONIBLE (total sistema)
    "pot_indisponible": 463,

    # DISPONIBLE (por tecnología)
    "pot_disp_hidraulica": 472,
    "pot_disp_nuclear": 474,
    "pot_disp_carbon": 475,
    "pot_disp_ciclo_combinado": 477,
    "pot_disp_fuel": 478,
    "pot_disp_gas": 479,

    # Eólica y Solar (renovables no siempre están en este bloque convencional)
    "pot_disp_eolica": 473,
    "pot_disp_solar": 476,

    # INSTALADA (por tecnología)
    "pot_inst_hidraulica": 464,
    "pot_inst_eolica": 465,
    "pot_inst_nuclear": 466,
    "pot_inst_carbon": 467,
    "pot_inst_ciclo_combinado": 468,
    "pot_inst_fuel": 469,
    "pot_inst_gas": 470,
    "pot_inst_solar": 471
}