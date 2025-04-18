import os
import requests
from fredapi import Fred
import pandas as pd

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")  # Your Dune API key
DUNE_TABLE = "us_retail_consumption"      # Your target table name or identifier

fred = Fred(api_key=FRED_API_KEY)

def get_retail_consumption():
    data = fred.get_series('RSXFS')
    df = pd.DataFrame({'date': data.index.strftime('%Y-%m-%d'), 'value': data.values})
    return df

def push_to_dune(df: pd.DataFrame, table_name: str, api_key: str):
    dune_api_url = f"https://api.dune.com/api/v1/table/{table_name}/insert"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "data": df.to_dict(orient="records")
    }
    response = requests.post(dune_api_url, json=payload, headers=headers)
    response.raise_for_status()
    print("✅ Data pushed to Dune successfully.")

# === Main Script ===
df = get_retail_consumption()
push_to_dune(df, DUNE_TABLE, DUNE_API_KEY)
