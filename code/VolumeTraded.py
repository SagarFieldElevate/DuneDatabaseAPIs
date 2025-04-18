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

# === Fetch 365 days of volume data ===
data = []
for symbol, coin_id in coins.items():
    market_data = cg.get_coin_market_chart_by_id(id=coin_id, vs_currency='usd', days=365)
    volume_data = market_data['total_volumes']
    
    for timestamp_ms, volume in volume_data:
        data.append({
            'symbol': symbol,
            'date': datetime.utcfromtimestamp(timestamp_ms / 1000).date().isoformat(),
            'volume_usd': volume
        })

# === Save to CSV ===
df = pd.DataFrame(data)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = f"crypto_volume_traded_365_days_{timestamp}.csv"
df.to_csv(filename, index=False)

# === Push to Dune ===
print("📤 Uploading volume traded data to Dune...")
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "crypto_volume_traded"
DUNE_DESCRIPTION = "Crypto Volume Traded Data from CoinGecko."

headers = {
    "X-DUNE-API-KEY": DUNE_API_KEY,
    "Content-Type": "application/json"
}
payload = {
    "data": open(filename, "r").read(),
    "description": DUNE_DESCRIPTION,
    "table_name": DUNE_TABLE_NAME,
    "is_private": False
}

response = requests.post(dune_url, json=payload, headers=headers)
response.raise_for_status()

# Cleanup
os.remove(filename)
print("✅ Volume traded data successfully uploaded to Dune Analytics.")
