# === Global Money Supply to Dune ===
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
from fredapi import Fred

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "global_money_supply"
DUNE_DESCRIPTION = "Global Money Supply (M2/M3) across US, China, India, South Korea in USD"

# === Currency Conversion Rates to USD (fixed for simplicity) ===
INR_TO_USD = 0.012  # ₹1 = $0.012
CNY_TO_USD = 0.14   # ¥1 = $0.14
KRW_TO_USD = 0.00073  # ₩1 = $0.00073

# === FRED US M2 ===
def get_us_m2():
    fred = Fred(api_key=FRED_API_KEY)
    end = datetime.now()
    start = end - timedelta(days=3650)
    data = fred.get_series('M2SL', observation_start=start, observation_end=end)
    df = pd.DataFrame({'date': data.index, 'money_supply': data.values / 1000})  # billions USD
    df['country'] = 'US'
    return df

# === India M3 from Excel ===
def get_india_m3():
    path = 'data/RBIB Table No. 07 _ Sources of Money Stock (M3).xlsx'
    df = pd.read_excel(path, skiprows=5)[['Date', 'M3 (1+2+3+4-5)']]
    df.columns = ['date', 'money_supply']
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['money_supply'] = pd.to_numeric(df['money_supply'].astype(str).str.replace(',', ''), errors='coerce')
    df.dropna(inplace=True)
    df = df[df['date'] >= datetime.now() - timedelta(days=3650)]
    df['money_supply'] = (df['money_supply'] * INR_TO_USD) / 1000  # billions USD
    df['country'] = 'India'
    return df

# === China CNM2 from CSV ===
def get_china_m2():
    path = 'data/ECONOMICS_CNM2, 1D.csv'
    df = pd.read_csv(path, usecols=['time', 'close'])
    df.columns = ['date', 'money_supply']
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] >= '2010-01-01']
    df['money_supply'] = (df['money_supply'] * CNY_TO_USD) / 1e9  # billions USD
    df['country'] = 'China'
    return df

# === Korea KRM2 from CSV ===
def get_korea_m2():
    path = 'data/ECONOMICS_KRM2, 1D.csv'
    df = pd.read_csv(path, usecols=['time', 'close'])
    df.columns = ['date', 'money_supply']
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] >= '2010-01-01']
    df['money_supply'] = (df['money_supply'] * KRW_TO_USD) / 1e9  # billions USD
    df['country'] = 'South Korea'
    return df

# === Combine & Upload ===
def push_to_dune():
    df = pd.concat([
        get_us_m2(),
        get_india_m3(),
        get_china_m2(),
        get_korea_m2()
    ])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.drop(columns=['currency'], errors='ignore', inplace=True)

    payload = {
        "data": df.to_csv(index=False),
        "description": DUNE_DESCRIPTION,
        "table_name": DUNE_TABLE_NAME,
        "is_private": False
    }

    response = requests.post(
        "https://api.dune.com/api/v1/table/upload/csv",
        json=payload,
        headers={
            "X-DUNE-API-KEY": DUNE_API_KEY,
            "Content-Type": "application/json"
        }
    )

    response.raise_for_status()
    print("✅ Global money supply data pushed to Dune successfully.")

# === Run ===
push_to_dune()
