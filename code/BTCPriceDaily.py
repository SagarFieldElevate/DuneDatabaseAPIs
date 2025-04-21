import os
import yfinance as yf
import pandas as pd
import requests
from io import StringIO

# === Dune Config ===
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "btc_daily_close_price"

# === Fetch BTC data from yfinance (max duration) ===
symbol = "BTC-USD"
df = yf.download(symbol, period="max")[['Close']].reset_index()

# === Rename columns and format ===
df.columns = ['date', 'close_price_usd']
df['date'] = df['date'].dt.strftime('%Y-%m-%d')  # Format date as string

# === Upload to Dune ===
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
        "description": "Daily BTC closing price from Yahoo Finance (Max Duration)",
        "table_name": table_name,
        "is_private": False
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    print("✅ Uploaded to Dune:", table_name)

upload_csv_to_dune(df, DUNE_TABLE_NAME, DUNE_API_KEY)
