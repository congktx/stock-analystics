from datetime import datetime
from config import GlobalConfig
import requests
import json
import pandas as pd

def date_to_timestamp(year: int, 
                 month: int, 
                 day: int, 
                 hour: int = 0,
                 minute: int = 0,
                 second: int = 0) -> int:
    """Convert a datetime object to Unix timestamp in milliseconds."""
    date = datetime(year=year, 
                    month=month, 
                    day=day, 
                    hour=hour, 
                    minute=minute, 
                    second=second)
    return int(date.timestamp() * 1000)

def make_date(year: int, month: int, day: int) -> str:
    return f"{year:04d}-{month:02d}-{day:02d}"

def pull_daily_data(from_timestamp: int, to_timestamp: int):

    url = GlobalConfig.API_OHLC_DATA_URL
    headers = {
        "content-type": "application/json; charset=utf8"
    }

    params = {
        "from_timestamp": from_timestamp,
        "to_timestamp": to_timestamp,
        "limit": 200,
        "page": 1
    }

    try:
        results = []
        result: dict = requests.get(url=url, 
                       headers=headers,
                       params=params).json()
        data = result.get("documents", [])
        page_id = result.get("page_id", 1)
        page_count = result.get("page_count", 1)

        results += data
        while page_id < page_count:
            page_id += 1
            params.update({"page": page_id})
            try:
                result: dict = requests.get(url=url, 
                       headers=headers,
                       params=params).json()
                data = result.get("documents", [])
                page_id = result.get("page_id", 1)
                page_count = result.get("page_count", 1)
                results += data

            except Exception as e:
                print(f"Error while pull ohlc data {e}-> page {page_id}")
        df = pd.DataFrame(results)
        if "timestamp" in df.columns:
            df = df.drop(columns=["timestamp"])
        df.to_csv("data/output.csv",index=False, header=True)
        df.to_parquet('data/output.parquet', index=False)
        
    except Exception as e:
        print(f"Error while pull ohlc data {e}-> page 1")