import requests
import pandas as pd
from config.config import HEADERS

URL = "https://api.esios.ree.es/indicators"

response = requests.get(URL, headers=HEADERS)

data = response.json()

indicadores = data["indicators"]

df = pd.DataFrame(indicadores)

# Nos quedamos con lo importante
df = df[[
    "id",
    "name",
    "short_name",
    "description"
]]

# 🔍 Filtrar por palabras clave
filtro = df[
    df["name"].str.contains("generaci", case=False, na=False) |
    df["description"].str.contains("generaci", case=False, na=False)
]

print(filtro.sort_values("name").head(50))

# Guardar para exploración manual
df.to_csv("data/catalogo_indicadores.csv", index=False)

print("\n✅ Catálogo guardado en data/catalogo_indicadores.csv")