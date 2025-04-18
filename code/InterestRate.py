import pandas as pd
from datetime import datetime
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "fed_interest_rate"
DUNE_DESCRIPTION = "Fed Interest Rate data (FEDFUNDS) from FRED"

# === Fetch Interest Rate Data ===
fred = Fred(api_key=FRED_API_KEY)
data = fred.get_series('FEDFUNDS')
df = pd.DataFrame({'date': data.index, 'fed_funds_rate': data.values})

# === Convert to CSV String ===
csv_data = df.to_csv(index=False)

# === Push to Dune ===
print("📤 Uploading Fed Interest Rate data to Dune...")
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

print("✅ Fed Interest Rate data pushed to Dune successfully.")
