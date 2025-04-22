# === Global M2 (Common Dates Only) to Dune ===
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
from fredapi import Fred

# === Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "global_m2_usd"
DUNE_DESCRIPTION = "Daily global M2 in USD across US, China, India, and South Korea (only on common dates)"

# === Exchange Rates ===
INR_TO_USD = 0.012
CNY_TO_USD = 0.14
KRW_TO_USD = 0.00073

# === US M2 from FRED ===
def get_us_m2():
    fred = Fred(api_key=FRED_API_KEY)
    data = fred.get_series('M2SL', observation_start=datetime.now() - timedelta(days=3650))
    df = pd.DataFrame({'date': data.index, 'usd': data.values / 1000})  # billions
    return df

# === India M3 from Excel ===
def get_india_m3():
    df = pd.read_excel("data/RBIB Table No. 07 _ Sources of Money Stock (M3).xlsx", skiprows=5)
    df = df[['Date', 'M3 (1+2+3+4-5)']].dropna()
    df.columns = ['date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    df['usd'] = pd.to_numeric(df['value'].astype(str).str.replace(',', ''), errors='coerce') * INR_TO_USD / 1000
    return df[['date', 'usd']].dropna()

# === China CNM2 from CSV ===
def get_china_m2():
    df = pd.read_csv("data/ECONOMICS_CNM2, 1D.csv", usecols=['time', 'close'])
    df.columns = ['date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    df['usd'] = df['value'] * CNY_TO_USD / 1e9
    return df[['date', 'usd']]

# === Korea KRM2 from CSV ===
def get_korea_m2():
    df = pd.read_csv("data/ECONOMICS_KRM2, 1D.csv", usecols=['time', 'close'])
    df.columns = ['date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    df['usd'] = df['value'] * KRW_TO_USD / 1e9
    return df[['date', 'usd']]

# === Combine and Calculate Global M2 ===
def compute_global_m2():
    us = get_us_m2().rename(columns={'usd': 'usd_us'})
    india = get_india_m3().rename(columns={'usd': 'usd_in'})
    china = get_china_m2().rename(columns={'usd': 'usd_cn'})
    korea = get_korea_m2().rename(columns={'usd': 'usd_kr'})

    # Round dates
    for df in [us, india, china, korea]:
        df['date'] = df['date'].dt.floor('D')

    # Merge only on common dates
    merged = us.merge(india, on='date') \
               .merge(china, on='date') \
               .merge(korea, on='date')

    # Sum all USD columns
    merged['global_m2'] = merged[['usd_us', 'usd_in', 'usd_cn', 'usd_kr']].sum(axis=1)
    return merged[['date', 'global_m2']].sort_values('date')

# === Upload to Dune ===
def push_to_dune(df):
    csv_data = df.to_csv(index=False)
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
    print("✅ Global M2 successfully pushed to Dune.")

# === Run ===
df_global = compute_global_m2()
push_to_dune(df_global)
