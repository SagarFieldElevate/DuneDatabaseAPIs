import pandas as pd
from datetime import datetime
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
fred = Fred(api_key=FRED_API_KEY)

# === Indicator Fetch Function ===
def get_unemployment():
    data = fred.get_series('UNRATE')
    return pd.DataFrame({'Date': data.index, 'Unemployment_Rate': data.values})

# === Main Script ===
df = get_unemployment()
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = f"us_unemployment_data_{timestamp}.csv"
df.to_csv(filename, index=False)

# === Push to Dune ===
print("📤 Uploading Unemployment Rate data to Dune...")
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "unemployment_data"
DUNE_DESCRIPTION = "US Unemployment Rate data from FRED."

headers = {
    "X-DUNE-API-KEY": DUNE_API_KEY,
    "Content-Type": "application/json"
}
payload = {
    "data": open(filename, "r").read(),
    "description": DUNE_DESCRIPTION,
    "table_name": DUNE_TABLE_NAME,
    "is_private": False
}

response = requests.post(dune_url, json=payload, headers=headers)
response.raise_for_status()

# Cleanup
os.remove(filename)
print("✅ Unemployment Rate data successfully uploaded to Dune Analytics.")
