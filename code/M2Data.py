import pandas as pd
from datetime import datetime, timedelta
from fredapi import Fred
import os, hashlib, requests

# === Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "global_money_supply"
DUNE_DESCRIPTION = "M2/M3 money supply for US, China, South Korea, and India"

# === File Paths ===
INDIA_PATH = "./data/RBIB Table No. 07 _ Sources of Money Stock (M3).xlsx"
CHINA_PATH = "./data/ECONOMICS_CNM2, 1D.csv"
KOREA_PATH = "./data/ECONOMICS_KRM2, 1D.csv"

def generate_id(country, date):
    uid = f"{country}_{pd.to_datetime(date).strftime('%Y%m%d')}"
    return hashlib.md5(uid.encode()).hexdigest()[:12]

# === US M2 (FRED) ===
def get_us_m2():
    fred = Fred(api_key=FRED_API_KEY)
    start = datetime.now() - timedelta(days=3650)
    data = fred.get_series('M2SL', observation_start=start)
    df = pd.DataFrame({'date': data.index, 'money_supply': data.values})
    df['money_supply'] /= 1000
    df['country'], df['money_supply_type'], df['currency'] = 'US', 'M2', 'USD'
    return df

# === India M3 (Excel) ===
def get_india_m3(path):
    df = pd.read_excel(path, skiprows=5)[['Date', 'M3 (1+2+3+4-5)']]
    df.columns = ['date', 'money_supply']
    df['money_supply'] = pd.to_numeric(df['money_supply'].astype(str).str.replace(',', ''), errors='coerce') / 1e3
    df['country'], df['money_supply_type'], df['currency'] = 'India', 'M3', 'INR'
    return df.dropna()

# === China & Korea M2 (CSV) ===
def get_m2_from_csv(path, country, currency):
    df = pd.read_csv(path, usecols=['time', 'close']).rename(columns={'time': 'date', 'close': 'money_supply'})
    df['date'] = pd.to_datetime(df['date'])
    df['money_supply'] /= 1e9
    df['country'], df['money_supply_type'], df['currency'] = country, 'M2', currency
    return df

# === Combine & Push ===
df = pd.concat([
    get_us_m2(),
    get_india_m3(INDIA_PATH),
    get_m2_from_csv(CHINA_PATH, "China", "CNY"),
    get_m2_from_csv(KOREA_PATH, "South Korea", "KRW")
])

df = df[df['date'] >= pd.Timestamp("2010-01-01")]
df['id'] = df.apply(lambda x: generate_id(x['country'], x['date']), axis=1)
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
df = df[['id', 'date', 'country', 'money_supply_type', 'money_supply', 'currency']]

# === Push to Dune ===
headers = {
    "X-DUNE-API-KEY": DUNE_API_KEY,
    "Content-Type": "application/json"
}
payload = {
    "data": df.to_csv(index=False),
    "description": DUNE_DESCRIPTION,
    "table_name": DUNE_TABLE_NAME,
    "is_private": False
}
res = requests.post("https://api.dune.com/api/v1/table/upload/csv", json=payload, headers=headers)
res.raise_for_status()
print("✅ Money supply data pushed to Dune successfully.")
