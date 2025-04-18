from pycoingecko import CoinGeckoAPI
import pandas as pd
from datetime import datetime
import os
import requests

# === Initialize CoinGecko ===
cg = CoinGeckoAPI()

# === Define coin IDs ===
coins = {
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
    'ADA': 'cardano',
    'SOL': 'solana',
    'DOT': 'polkadot',
    'AVAX': 'avalanche-2'
}

# === Fetch and calculate volatility and trading range ===
data = []
for symbol, coin_id in coins.items():
    market_data = cg.get_coin_market_chart_by_id(id=coin_id, vs_currency='usd', days=365)
    ohlc_data = market_data['prices']
    
    for i in range(1, len(ohlc_data)):
        prev_day = ohlc_data[i-1]
        current_day = ohlc_data[i]
        
        prev_timestamp, prev_price = prev_day
        current_timestamp, current_price = current_day
        
        high = max(prev_price, current_price)
        low = min(prev_price, current_price)
        
        volatility = ((high - low) / low) * 100 if low > 0 else 0
        trading_range = high - low
        
        data.append({
            'symbol': symbol,
            'timestamp': datetime.utcfromtimestamp(current_timestamp / 1000).isoformat(),
            'high_24h_usd': high,
            'low_24h_usd': low,
            'volatility_24h_%': round(volatility, 2),
            'trading_range_24h_usd': round(trading_range, 2)
        })

# === Save to CSV string ===
df = pd.DataFrame(data)
csv_data = df.to_csv(index=False)

# === Push to Dune ===
print("📤 Uploading volatility and trading range data to Dune...")
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "volatility_trading_range_data"
DUNE_DESCRIPTION = "365-Day Volatility and Trading Range data for various cryptocurrencies."

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

print("✅ Volatility and trading range data successfully uploaded to Dune Analytics.")
