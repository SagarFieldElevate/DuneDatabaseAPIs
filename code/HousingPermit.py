import os
import requests
import pandas as pd
from io import StringIO

FRED_API_KEY = os.getenv("FRED_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_TABLE_NAME = "housing_permits_data"
FRED_SERIES_ID = "PERMIT"  # This is the series ID for US Housing Permits

# === Fetch Housing Permits data from FRED ===
def fetch_housing_permits_data(fred_api_key, series_id):
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": fred_api_key,
        "file_type": "json",
        "frequency": "m",  # Monthly data
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data['observations'])
    df['date'] = pd.to_datetime(df['date'])
    df['value'] = pd.to_numeric(df['value'])
    df.columns = ['date', 'housing_permits']
    return df

df = fetch_housing_permits_data(FRED_API_KEY, FRED_SERIES_ID)

# === Upload to Dune ===
def upload_csv_to_dune(df, table_name, api_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    url = "https://api.dune.com/api/v1/table/upload/csv"
    headers = {
        "X-DUNE-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "data": csv_data,
        "description": f"US Housing Permits data from FRED",
        "table_name": table_name,
        "is_private": False
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    print(f"✅ Uploaded to Dune: {table_name}")

upload_csv_to_dune(df, DUNE_TABLE_NAME, DUNE_API_KEY)
