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

# === Fetch 365 days of market cap data ===
data = []
for symbol, coin_id in coins.items():
    # Fetch market data (price and volume) for the last 365 days
    market_data = cg.get_coin_market_chart_by_id(id=coin_id, vs_currency='usd', days=365)

    # Fetch circulating supply (constant for each coin)
    coin_data = cg.get_coin_by_id(id=coin_id, localization=False)
    circulating_supply = coin_data['market_data']['circulating_supply']
    
    # Process price and volume data
    price_data = market_data['prices']
    volume_data = market_data['total_volumes']
    
    # Prepare DataFrame with timestamp, price, volume, and market cap
    df = pd.DataFrame(price_data, columns=['timestamp', 'price'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
    df.drop(columns=['timestamp'], inplace=True)
    
    # Add volume and calculate market cap
    df['volume'] = [volume[1] for volume in volume_data]
    df['market_cap'] = df['price'] * circulating_supply
    df['symbol'] = symbol
    
    # Append data for this coin
    data.append(df)

# === Combine data into one DataFrame ===
combined_df = pd.concat(data, ignore_index=True)

# === Convert to CSV String ===
csv_data = combined_df.to_csv(index=False)

# === Push to Dune ===
print("📤 Uploading Market Cap data to Dune...")
dune_url = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "market_cap_data"
DUNE_DESCRIPTION = "Historical Market Cap data for various cryptocurrencies."

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

print("✅ Market Cap data pushed to Dune Analytics successfully.")
