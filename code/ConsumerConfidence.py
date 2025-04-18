import pandas as pd
from datetime import datetime
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "consumer_confidence"
DUNE_DESCRIPTION = "University of Michigan Consumer Sentiment Index (UMCSENT) from FRED"

fred = Fred(api_key=FRED_API_KEY)

# === Fetch Data ===
def get_consumer_confidence():
    data = fred.get_series('UMCSENT')
    return pd.DataFrame({
        'date': data.index.strftime('%Y-%m-%d'),
        'consumer_confidence': data.values
    })

df = get_consumer_confidence()

# === Convert to CSV String for Dune Upload ===
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
print("✅ Consumer Confidence data pushed to Dune successfully.")
