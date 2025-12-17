from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import cursor as Cursor
from datetime import datetime
from config import GlobalConfig
import pandas as pd
import requests
import json
import pandas as pd
import csv
import logging

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

def pull_daily_ohlc_data(from_timestamp: int, to_timestamp: int):

    url = GlobalConfig.API_OHLC_DATA_URL
    headers = {
        "content-type": "application/json; charset=utf8"
    }

    params = {
        "from_timestamp": from_timestamp,
        "to_timestamp": to_timestamp,
        "limit": 2000,
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
        df['ticker'] = df['ticker'].astype(str)
        df.to_csv("/workspace/airflow/data/output.csv",index=False, header=True)
        df.to_parquet('/workspace/airflow/data/output.parquet', index=False)
        
    except Exception as e:
        print(f"Error while pull ohlc data {e}-> page 1")

def pull_companies_data():
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    cursor: Cursor = conn.cursor()
    cursor.execute(
        """
        select 
            company_name,
            company_ticker,
            company_asset_type,
            company_composite_figi,
            company_cik,
            company_industry,
            company_sic_code
        from datasource.companies
        """
    )

    with open("./data/companies.csv", 'w') as f:
        csr_writer = csv.writer(f)
        headers = [desc[0] for desc in cursor.description]
        csr_writer.writerow(headers)
        csr_writer.writerows(cursor.fetchall())
    
    cursor.close()
    conn.close()
    logging.info("Saved companies data in text file companies.txt")

    df = pd.read_csv("./data/companies.csv",
                        dtype={
                            "company_cik": "string",
                            "company_sic_code": "string"
                        }
                    )
    df.to_parquet("./data/companies.parquet", index=False)