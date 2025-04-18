from pycoingecko import CoinGeckoAPI
import pandas as pd
from datetime import datetime
import os
import requests

# === Config ===
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "trending_coins"
DUNE_DESCRIPTION = "Trending coins from CoinGecko API (get_search_trending)"

# === Fetch Trending Coins ===
cg = CoinGeckoAPI()
print("🔍 Fetching trending coins from CoinGecko...")
trending_data = cg.get_search_trending()

# === Format Data ===
coin_list = []
for coin_info in trending_data['coins']:
    coin = coin_info.get('item', {})
    coin_list.append({
        'name': coin.get('name'),
        'symbol': coin.get('symbol'),
        'id': coin.get('id'),
        'market_cap_rank': coin.get('market_cap_rank'),
        'score': coin.get('score')
    })

df = pd.DataFrame(coin_list)

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

print("✅ Trending coins pushed to Dune successfully.")
