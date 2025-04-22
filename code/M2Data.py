# === Global M2 Sum to Dune ===
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "global_m2_usd"
DUNE_DESCRIPTION = "Daily global M2 (US, China, India, South Korea) in USD"

# === Fixed Currency Rates to USD ===
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
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['value'] = pd.to_numeric(df['value'].astype(str).str.replace(',', ''), errors='coerce')
    df = df[df['date'] >= datetime.now() - timedelta(days=3650)]
    df['usd'] = (df['value'] * INR_TO_USD) / 1000  # billions
    return df[['date', 'usd']]

# === China CNM2 from CSV ===
def get_china_m2():
    df = pd.read_csv("data/ECONOMICS_CNM2, 1D.csv", usecols=['time', 'close'])
    df.columns = ['date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    df['usd'] = (df['value'] * CNY_TO_USD) / 1e9  # billions
    return df[['date', 'usd']]

# === Korea KRM2 from CSV ===
def get_korea_m2():
    df = pd.read_csv("data/ECONOMICS_KRM2, 1D.csv", usecols=['time', 'close'])
    df.columns = ['date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    df['usd'] = (df['value'] * KRW_TO_USD) / 1e9  # billions
    return df[['date', 'usd']]

# === Combine and Aggregate ===
def get_global_m2():
    df_all = pd.concat([
        get_us_m2(),
        get_india_m3(),
        get_china_m2(),
        get_korea_m2()
    ])
    df_all = df_all.groupby('date', as_index=False).agg(global_m2=('usd', 'sum'))
    df_all['date'] = df_all['date'].dt.strftime('%Y-%m-%d')
    return df_all.sort_values('date')

# === Push to Dune ===
def push_to_dune():
    df = get_global_m2()
    payload = {
        "data": df.to_csv(index=False),
        "description": DUNE_DESCRIPTION,
        "table_name": DUNE_TABLE_NAME,
        "is_private": False
    }
    headers = {
        "X-DUNE-API-KEY": DUNE_API_KEY,
        "Content-Type": "application/json"
    }
    res = requests.post("https://api.dune.com/api/v1/table/upload/csv", json=payload, headers=headers)
    res.raise_for_status()
    print("✅ Global M2 (USD) pushed to Dune successfully.")

# === Run ===
push_to_dune()
