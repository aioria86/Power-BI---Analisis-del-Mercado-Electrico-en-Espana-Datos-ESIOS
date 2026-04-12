# 📊 Data Pipeline - Proyecto Mercado Eléctrico (ESIOS)

Los datos no se incluyen en el repositorio debido a su tamaño.

Este proyecto construye un pipeline completo de datos basado en la API de ESIOS para analizar el sistema eléctrico español.

---

# 🔐 1. Requisitos previos

## Token ESIOS

Para poder descargar los datos es necesario disponer de un token de acceso a la API de ESIOS.

Pasos:

1. Registrarse en:
   https://www.esios.ree.es

2. Obtener el token personal

3. Configurarlo en el archivo: config/config.py


Ejemplo:

```python
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": "TU_TOKEN_AQUI"
}

```

# Descarga de datos

## Opción 1: Descarga estándar (por año)

python -m scripts.esios_downloader


## Opción 2: Descarga mensual (recomendada para ciertos indicadores)

python -m scripts.esios_downloader_mensual


Este script descarga los indicadores definidos en: config/indicadores.py


Agrupa los datos por año y los guarda en formato parquet: data/raw/esios/


# Construcción de tablas de hechos

## Fact principal del sistema eléctrico

python -m scripts.build_fact_energia_full_v2

Genera: data/processed/fact_energia_full_v2.parquet


## Fact de precios spot internacionales

python -m scripts.build_fact_precio_spot

Genera: data/processed/fact_precio_spot.parquet


# Estructura del pipeline

scripts/
│
├── esios_downloader.py
├── esios_downloader_mensual.py
├── build_fact_energia_full_v2.py
├── build_fact_precio_spot.py
│
config/
│
├── config.py
├── indicadores.py


# Modelo de datos generado

El pipeline construye un modelo analítico basado en:

Tablas de hechos:

fact_energia_full_v2

fact_precio_spot

Dimensiones:

calendario

hora

país


# Consideraciones importantes

Algunos indicadores no tienen histórico completo (especialmente ERNI)

La API puede devolver datos vacíos en ciertos años

Se recomienda trabajar con datos recientes (2023–2025)

Los datos horarios se agregan según el tipo:

Energía → SUM

Precios → AVERAGE


# Flujo recomendado

1. Configurar token

2. Ejecutar downloader (normal o mensual)

3. Construir tablas:

      fact_energia

   fact_precio_spot

4. Importar en Power BI

5. Crear modelo estrella






