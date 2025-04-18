# === Global GDP Data to Dune ===
import pandas as pd
from datetime import datetime
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "global_gdp"
DUNE_DESCRIPTION = "World GDP (current US$) from FRED (NYGDPMKTPCDWLD)"

fred = Fred(api_key=FRED_API_KEY)

# === Fetch Global GDP Data ===
def get_global_gdp():
    data = fred.get_series('NYGDPMKTPCDWLD')
    df = pd.DataFrame({
        'date': data.index.strftime('%Y-%m-%d'),
        'world_gdp_usd': data.values
    })
    return df

df = get_global_gdp()

# === Convert to CSV String ===
csv_data = df.to_csv(index=False)

# === Push to Dune ===
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
headers = {
    "X-DUNE-API-KEY": DUNE_API_KEY,
    "Content-Type": "application/json"
}
payload = {
    "data": csv_data,
    "description": DUNE_DESCRIPTION,
    "table_name": DUNE_TABLE_NAME,
    "is_private": False
}

response = requests.post(dune_url, json=payload, headers=headers)
response.raise_for_status()

print("✅ Global GDP data pushed to Dune successfully.")
