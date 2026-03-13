# Análisis del Mercado Eléctrico en España — Datos ESIOS

## Descripción del proyecto

Este proyecto analiza el sistema eléctrico español utilizando datos abiertos de **ESIOS (Red Eléctrica de España)**.

El objetivo es construir un **dashboard interactivo en Power BI** que permita explorar la evolución de:

* La demanda eléctrica
* La generación de energía por tecnología
* Los precios del mercado eléctrico
* Los intercambios internacionales de electricidad
* La integración de energías renovables en el sistema eléctrico

El proyecto demuestra cómo los **datos abiertos del sistema energético** pueden transformarse en información analítica útil mediante herramientas de **Business Intelligence**.

---

# Fuente de datos

Los datos provienen de la plataforma oficial de transparencia del sistema eléctrico español:

**ESIOS — Red Eléctrica de España**

https://www.esios.ree.es

Esta plataforma ofrece datasets abiertos sobre:

* Generación eléctrica por tecnología
* Demanda eléctrica
* Precios del mercado eléctrico
* Intercambios internacionales de electricidad
* Indicadores de energías renovables
* Gestión de la demanda eléctrica

Los datasets pueden descargarse manualmente o consumirse mediante la API de ESIOS.

---

# Objetivos del proyecto

Los principales objetivos de este proyecto son:

* Construir un **modelo de datos energético estructurado**
* Crear dashboards interactivos en Power BI
* Analizar tendencias históricas en la demanda eléctrica
* Explorar el mix energético en España
* Analizar la evolución del precio de la electricidad
* Estudiar los intercambios eléctricos internacionales
* Identificar patrones en la generación de energías renovables

---

# Secciones del dashboard

El dashboard de Power BI se estructura en varias páginas analíticas.

### 1️⃣ Demanda eléctrica

* Demanda real vs demanda prevista
* Evolución horaria de la demanda
* Patrones diarios de consumo eléctrico

### 2️⃣ Mix de generación energética

* Generación por tecnología
* Energía renovable vs no renovable
* Evolución de energía solar y eólica

### 3️⃣ Precios del mercado eléctrico

* Precio spot de la electricidad
* Evolución histórica del precio
* Volatilidad del mercado eléctrico

### 4️⃣ Intercambios internacionales

* Flujos eléctricos con Francia
* Flujos eléctricos con Portugal
* Balance de importación y exportación de energía

### 5️⃣ Integración de energías renovables

* Participación de energías renovables
* Energía renovable no integrable
* Indicadores de flexibilidad del sistema eléctrico

---

# Tecnologías utilizadas

* **Power BI**
* **Power Query**
* **DAX**
* **Python** (opcional para procesamiento de datos)

---

# Estructura del proyecto

data/ → datasets crudos y procesados
powerbi/ → archivo del dashboard en Power BI
notebooks/ → análisis exploratorio
scripts/ → procesamiento y limpieza de datos
dashboards/ → capturas del dashboard
docs/ → documentación de datos

---

# Ejemplos de análisis realizados

Algunos análisis incluidos en el proyecto:

* Patrones estacionales de la demanda eléctrica
* Evolución de la generación renovable
* Volatilidad del precio de la electricidad
* Comercio eléctrico entre países
* Impacto de las energías renovables en el mercado eléctrico

---

# Posibles mejoras futuras

Este proyecto puede ampliarse con:

* Automatización de la descarga de datos mediante la API de ESIOS
* Dashboards en tiempo real
* Modelos de predicción de demanda eléctrica
* Modelos predictivos del precio de la electricidad

---

# Autor

Juan Manuel Pérez
Analista de Datos | Business Intelligence | Ciencia de Datos
