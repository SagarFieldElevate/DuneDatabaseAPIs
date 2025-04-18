import requests
import pandas as pd
from datetime import datetime
import os

# === Secrets & Config ===
COINDESK_API_KEY = os.getenv("COINDESK_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "coindesk_sentiment"
DUNE_DESCRIPTION = "Latest crypto sentiment articles from trusted sources via CoinDesk API"

# === API Setup ===
url = "https://data-api.coindesk.com/news/v1/article/list"
trusted_sources = [
    "coindesk", "cointelegraph", "blockworks", "decrypt", "bitcoinmagazine",
    "theblock", "bloomberg_crypto_", "forbes", "yahoofinance",
    "financialtimes_crypto_", "seekingalpha"
]

# === Collect Articles ===
articles = []
limit = 100
max_calls = 100
to_ts = int(datetime.utcnow().timestamp())

for call_count in range(1, max_calls + 1):
    print(f"API Call #{call_count}")
    params = {
        "api_key": COINDESK_API_KEY,
        "limit": limit,
        "to_ts": to_ts,
        "lang": "EN",
        "source_ids": ",".join(trusted_sources)
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        current_articles = response.json().get("Data", [])
        if not current_articles:
            break
        articles.extend(current_articles)
        to_ts = current_articles[-1].get('PUBLISHED_ON', to_ts) - 1
    except Exception as e:
        print(f"Request failed: {e}")
        break

# === Convert to DataFrame ===
df = pd.DataFrame([{
    "id": a.get("ID"),
    "url": a.get("URL"),
    "title": a.get("TITLE"),
    "body": a.get("BODY"),
    "sentiment": a.get("SENTIMENT"),
    "upvotes": a.get("UPVOTES"),
    "downvotes": a.get("DOWNVOTES"),
    "keywords": a.get("KEYWORDS"),
    "published_on": datetime.utcfromtimestamp(a.get("PUBLISHED_ON")).strftime('%Y-%m-%d') if a.get("PUBLISHED_ON") else None,  # Date formatted as "YYYY-MM-DD"
    "source_type": a.get("SOURCE_DATA", {}).get("SOURCE_TYPE"),
    "source_name": a.get("SOURCE_DATA", {}).get("NAME"),
    "benchmark_score": a.get("SOURCE_DATA", {}).get("BENCHMARK_SCORE"),
    "categories": "|".join([cat.get("CATEGORY") for cat in a.get("CATEGORY_DATA", [])])
} for a in articles])

# === Save as CSV for Dune Upload ===
csv_data = df.to_csv(index=False)

# === Push CSV to Dune ===
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
print("✅ CoinDesk Sentiment pushed to Dune successfully.")
