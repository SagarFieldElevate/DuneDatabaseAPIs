# === Global Money Supply Data to Dune ===
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
from fredapi import Fred
import hashlib
from io import StringIO

# === Secrets & Config ===
FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "global_money_supply_data"
DUNE_DESCRIPTION = "Global money supply data from FRED, Excel, and CSV sources"

fred = Fred(api_key=FRED_API_KEY)

# === Helper Function to Generate Record ID ===
def generate_record_id(country, date):
    """Generate a deterministic unique ID based on country and date"""
    date_str = pd.to_datetime(date).strftime('%Y%m%d')
    unique_string = f"{country}_{date_str}"
    return hashlib.md5(unique_string.encode()).hexdigest()[:12]

# === Fetch US M2 Data from FRED ===
def get_m2_data(fred_api_key, days=3650):
    fred = Fred(api_key=fred_api_key)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    m2_data = fred.get_series('M2SL', observation_start=start_date.strftime('%Y-%m-%d'), observation_end=end_date.strftime('%Y-%m-%d'))
    df = pd.DataFrame({'date': m2_data.index, 'money_supply': m2_data.values})
    df['money_supply'] = df['money_supply'] / 1000  # trillions -> billions
    df['country'] = 'US'
    df['money_supply_type'] = 'M2'
    df['currency'] = 'USD'
    # Generate unique IDs and set as index
    df['record_id'] = df.apply(lambda x: generate_record_id(x['country'], x['date']), axis=1)
    df.set_index('record_id', inplace=True)
    df.index.name = 'id'  # Dune expects 'id' as the primary key
    return df

# === Fetch India M3 Data from Excel ===
def get_m3_data(file_path):
    df = pd.read_excel(file_path, skiprows=5)
    df = df[['Date', 'M3 (1+2+3+4-5)']].rename(columns={'Date': 'date', 'M3 (1+2+3+4-5)': 'money_supply'})
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['money_supply'] = pd.to_numeric(df['money_supply'].astype(str).str.replace(',', ''), errors='coerce') / 1e3  # ₹ Cr to ₹ Billion
    df = df.dropna()
    latest_date = df['date'].max()
    start_date = latest_date - timedelta(days=3650)
    df = df[df['date'] >= start_date]
    df['country'] = 'India'
    df['money_supply_type'] = 'M3'
    df['currency'] = 'INR'
    df['record_id'] = df.apply(lambda x: generate_record_id(x['country'], x['date']), axis=1)
    df.set_index('record_id', inplace=True)
    df.index.name = 'id'
    return df

# === Fetch China CNM2 Data from CSV ===
def get_cnm2_data(csv_path):
    df = pd.read_csv(csv_path, usecols=['time', 'close'])
    df.rename(columns={'time': 'date', 'close': 'money_supply'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df['money_supply'] = df['money_supply'] / 1e9  # from units to billions
    df = df[df['date'] >= '2010-01-01']
    df['country'] = 'China'
    df['money_supply_type'] = 'M2'
    df['currency'] = 'CNY'
    df['record_id'] = df.apply(lambda x: generate_record_id(x['country'], x['date']), axis=1)
    df.set_index('record_id', inplace=True)
    df.index.name = 'id'
    return df

# === Fetch South Korea KRM2 Data from CSV ===
def get_krm2_data(csv_path):
    df = pd.read_csv(csv_path, usecols=['time', 'close'])
    df.rename(columns={'time': 'date', 'close': 'money_supply'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df['money_supply'] = df['money_supply'] / 1e9  # from units to billions
    df = df[df['date'] >= '2010-01-01']
    df['country'] = 'South Korea'
    df['money_supply_type'] = 'M2'
    df['currency'] = 'KRW'
    df['record_id'] = df.apply(lambda x: generate_record_id(x['country'], x['date']), axis=1)
    df.set_index('record_id', inplace=True)
    df.index.name = 'id'
    return df

# === Combine All Data ===
def combine_all_data(fred_api_key, india_path, china_path, korea_path):
    """Combine all data sources into one DataFrame"""
    m2_df = get_m2_data(fred_api_key)
    m3_df = get_m3_data(india_path)
    cnm2_df = get_cnm2_data(china_path)
    krm2_df = get_krm2_data(korea_path)
    
    # Combine all DataFrames
    combined_df = pd.concat([m2_df, m3_df, cnm2_df, krm2_df])
    
    # Ensure date format is consistent
    combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime('%Y-%m-%d')
    
    # Reset index to ensure 'id' column is included
    combined_df.reset_index(inplace=True)
    
    return combined_df

# === Push to Dune ===
def upload_to_dune(df, table_name, description, api_key):
    """Push combined DataFrame to Dune Analytics"""
    url = "https://api.dune.com/api/v1/table/upload/csv"
    headers = {
        "X-DUNE-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "data": df.to_json(orient="records"),  # Send the data as JSON
        "description": description,
        "table_name": table_name,
        "is_private": False
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    print(f"✅ Data pushed to Dune: {table_name}")

# === Run Everything ===
# Get and combine all data
combined_data = combine_all_data(FRED_API_KEY, "path_to_india_m3_file", "path_to_china_cnm2_file", "path_to_korea_krm2_file")

# Upload combined data to Dune
upload_to_dune(combined_data, DUNE_TABLE_NAME, DUNE_DESCRIPTION, DUNE_API_KEY)
