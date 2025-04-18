# === US GDP Data to Dune ===
import pandas as pd
from datetime import datetime
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "us_gdp"
DUNE_DESCRIPTION = "Quarterly US Gross Domestic Product (GDP) from FRED (GDP)"

fred = Fred(api_key=FRED_API_KEY)

# === Fetch US GDP Data ===
def get_gdp():
    data = fred.get_series('GDP')
    df = pd.DataFrame({
        'date': data.index.strftime('%Y-%m-%d'),
        'us_gdp': data.values
    })
    return df

df = get_gdp()

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

print("✅ US GDP data pushed to Dune successfully.")
