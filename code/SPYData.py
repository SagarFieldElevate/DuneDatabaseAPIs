import os
import yfinance as yf
import pandas as pd
import requests
from io import StringIO

DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "spy_daily_close_price"
symbol = "SPY"

# === Fetch all SPY daily close data ===
df = yf.download(symbol, period="max")[['Close']].reset_index()
df.columns = ['date', 'close_price_usd']

# === Upload to Dune ===
def upload_csv_to_dune(df, table_name, api_key):
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
        "description": f"Full historical SPY daily close price from Yahoo Finance",
        "table_name": table_name,
        "is_private": False
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    print(f"✅ Uploaded to Dune: {table_name}")

upload_csv_to_dune(df, DUNE_TABLE_NAME, DUNE_API_KEY)
