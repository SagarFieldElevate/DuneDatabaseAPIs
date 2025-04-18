import os
import requests
import pandas as pd
from datetime import datetime
from pycoingecko import CoinGeckoAPI
from io import StringIO

# === Dune Config ===
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "crypto_365d_volatility_range"

# === Initialize CoinGecko ===
cg = CoinGeckoAPI()

# === Define coins ===
coins = {
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
    'ADA': 'cardano',
    'SOL': 'solana',
    'DOT': 'polkadot',
    'AVAX': 'avalanche-2'
}

# === Get 365d OHLC Data ===
data = []
for symbol, coin_id in coins.items():
    market_data = cg.get_coin_market_chart_by_id(id=coin_id, vs_currency='usd', days=365)
    prices = market_data['prices']
    
    for i in range(1, len(prices)):
        prev_time, prev_price = prices[i - 1]
        curr_time, curr_price = prices[i]
        
        high = max(prev_price, curr_price)
        low = min(prev_price, curr_price)
        volatility = ((high - low) / low) * 100 if low > 0 else 0
        trading_range = high - low

        data.append({
            'symbol': symbol,
            'timestamp': datetime.utcfromtimestamp(curr_time / 1000).isoformat(),
            'high_24h_usd': high,
            'low_24h_usd': low,
            'volatility_24h_%': round(volatility, 2),
            'trading_range_24h_usd': round(trading_range, 2)
        })

# === Convert to DataFrame ===
df = pd.DataFrame(data)

# === Upload CSV to Dune ===
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
        "description": "365-day volatility and trading range for major crypto assets from CoinGecko",
        "table_name": table_name,
        "is_private": False
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    print("✅ Uploaded to Dune:", table_name)

upload_csv_to_dune(df, DUNE_TABLE_NAME, DUNE_API_KEY)
