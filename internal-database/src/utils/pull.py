from utils.config import GlobalConfig
from datetime import datetime, timezone
from typing import Tuple
import requests, json
from pprint import pprint
import pandas as pd
from tqdm import tqdm
from utils.utils import (import_companies_table,
                         import_company_status_table)
from exports.export_company_status_to_csv import (export_data_of_companies_table,
                                                  export_data_of_company_status_table)


def date_to_timestamps(year: int, month: int) -> Tuple[int, int]:
    start = datetime(year, month, 1, 0, 0, 1, tzinfo=timezone.utc)
    start_ts = int(start.timestamp())

    if month == 12:
        month = 1
        year += 1
        end = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_ts = int(end.timestamp())
    else:
        end = datetime(year, month+1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_ts = int(end.timestamp())

    return start_ts, end_ts

def process_input_data():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()

    cursor.execute(query="""SELECT company_ticker from datasource.companies""")
    result = cursor.fetchall()
    tickers = set()

    for item in result:
        tickers.add(item[0])
    
    df = pd.read_csv(GlobalConfig.COMPANIES_TABLE_PATH)
    df = df.dropna(subset=["company_ticker"])
    df = df.drop_duplicates(subset=["company_ticker"])
    df = df[~df["company_ticker"].isin(tickers)]
    df.to_csv(GlobalConfig.COMPANIES_TABLE_PATH, index=False)
   
    df = pd.read_csv(GlobalConfig.COMPANY_STATUS_TABLE_PATH)
    df = df.dropna(subset=["company_status_ticker"])
    df.to_csv(GlobalConfig.COMPANY_STATUS_TABLE_PATH, index=False)
   
def pull_company_infos(year: int, month: int):
    start_ts, end_ts = date_to_timestamps(year, month)

    page_id = 1

    url = "http://localhost:8000/stock/company-infos"
    params = {
        "from_timestamp": start_ts,
        "to_timestamp": end_ts,
        "limit": 100,
        "page": page_id
    }

    results = []
    try:
        res:dict = requests.get(url=url, params=params).json()

        results += res.get("documents")

        page_count = res.get("page_count")

        for page_id in tqdm(range(2, page_count + 1), desc="Fetching pages", unit="page"):
            try:
                params["page"] = page_id
                res = requests.get(url=url, params=params).json()

                docs = res.get("documents", [])
                if docs:
                    results += docs
            except Exception as e:
                print(f"Error while fetching data: {e} -> page {page_id}")
    except Exception as e:
        print(f"Error while fetching data: {e} -> {page_id}")
    
    with open(GlobalConfig.COMPANY_INFOS_PATH, mode="w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False, indent=4)
    
    export_data_of_company_status_table()
    export_data_of_companies_table()
    process_input_data()
    import_companies_table()
    import_company_status_table()