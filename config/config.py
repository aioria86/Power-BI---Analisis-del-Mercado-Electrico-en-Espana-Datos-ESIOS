import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ESIOS_API_KEY")

BASE_URL = "https://api.esios.ree.es/indicators"

HEADERS = {
    "x-api-key": API_KEY,
    "Accept": "application/json"
}