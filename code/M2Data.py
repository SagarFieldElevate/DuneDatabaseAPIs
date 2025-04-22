# === US M2 Data to Dune ===
import pandas as pd
from datetime import datetime
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "us_m2"
DUNE_DESCRIPTION = "Weekly US M2 Money Supply from FRED (M2SL)"

fred = Fred(api_key=FRED_API_KEY)

# === Fetch US M2 Data ===
def get_us_m2():
    data = fred.get_series('M2SL')
    df = pd.DataFrame({
        'date': data.index.strftime('%Y-%m-%d'),
        'us_m2': data.values
    })
    return df

df = get_us_m2()

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

print("✅ US M2 data pushed to Dune successfully.")
