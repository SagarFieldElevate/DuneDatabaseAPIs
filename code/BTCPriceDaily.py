import os
import requests
import pandas as pd
from pycoingecko import CoinGeckoAPI
from io import StringIO

# === Dune Config ===
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "btc_daily_close_price"

# === Initialize CoinGecko ===
cg = CoinGeckoAPI()

# === Fetch 365d BTC prices ===
btc_market_data = cg.get_coin_market_chart_by_id(id='bitcoin', vs_currency='usd', days=365)

# === Convert to DataFrame and extract daily close ===
prices = btc_market_data['prices']
df = pd.DataFrame(prices, columns=["timestamp", "price"])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df.set_index('timestamp', inplace=True)

btc_daily_close = df.resample('1D').last().dropna().reset_index()
btc_daily_close.columns = ['date', 'close_price_usd']

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
        "description": "Daily BTC closing price from CoinGecko (past 365 days)",
        "table_name": table_name,
        "is_private": False
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    print("✅ Uploaded to Dune:", table_name)

upload_csv_to_dune(btc_daily_close, DUNE_TABLE_NAME, DUNE_API_KEY)
