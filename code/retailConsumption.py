import os
import requests
from fredapi import Fred
import pandas as pd
from io import StringIO

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
TABLE_NAME = "us_retail_consumption"

fred = Fred(api_key=FRED_API_KEY)

def get_retail_consumption():
    data = fred.get_series('RSXFS')
    df = pd.DataFrame({'DATE': data.index.strftime('%Y-%m-%d'), 'Retail_Sales_Ex_Auto': data.values})
    return df

def upload_csv_to_dune(df: pd.DataFrame, table_name: str, api_key: str):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    url = "https://api.dune.com/api/v1/table/upload/csv"
    headers = {
        "X-DUNE-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "data": csv_data,
        "description": "Retail sales ex-auto (RSXFS) from FRED",
        "table_name": table_name,
        "is_private": False  # or True if you want the table private
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    print("✅ CSV uploaded to Dune successfully.")

# === Run Script ===
df = get_retail_consumption()
upload_csv_to_dune(df, TABLE_NAME, DUNE_API_KEY)
