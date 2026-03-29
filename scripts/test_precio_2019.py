import requests
import pandas as pd
from config.config import HEADERS, BASE_URL

indicator_id = 600
nombre = "precio_mercado"

start_date = "2019-01-01"
end_date = "2019-12-31"

url = f"{BASE_URL}/{indicator_id}"

params = {
    "start_date": start_date,
    "end_date": end_date,
    "time_trunc": "hour"
}

response = requests.get(url, headers=HEADERS, params=params)

print(response.status_code)
print(response.text[:500])