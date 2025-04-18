# === Currency Exchange Data to Dune ===
import pandas as pd
from datetime import datetime
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "currency_exchange"
DUNE_DESCRIPTION = "Exchange rates: USD to CNY, EUR, and JPY from FRED (DEXCHUS, DEXUSEU, DEXJPUS)"

fred = Fred(api_key=FRED_API_KEY)

# === Fetch Currency Exchange Rates ===
def get_currency_exchange():
    cny = fred.get_series('DEXCHUS')  # USD to CNY
    eur = fred.get_series('DEXUSEU')  # USD to EUR
    jpy = fred.get_series('DEXJPUS')  # USD to JPY

    df = pd.DataFrame({
        'date': cny.index.strftime('%Y-%m-%d'),
        'usd_cny': cny.values,
        'usd_eur': eur.reindex(cny.index).values,
        'usd_jpy': jpy.reindex(cny.index).values
    })
    return df

df = get_currency_exchange()

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

print("✅ Currency Exchange data pushed to Dune successfully.")
