import time

import requests

from utils.time_utils import round_timestamp

from database.mongodb import MongoDB

from config import AlphavantageConfig

from utils.parse_timestamp import parse_date_to_timestamp

mongodb = MongoDB()


def get_market_status(date: str = "2024-12-01"):
    params = {
        "apikey": AlphavantageConfig.API_KEY,
        "function": "MARKET_STATUS"
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    url = 'https://www.alphavantage.co/query'

    try:
        response = requests.get(url, params=params, headers=headers).json()

        if not response.get('markets'):
            return response.get('errors')

        return response['markets']
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def load_all_market_status_to_db(list_market_status, time_update):
    for market_status in list_market_status:
        document = {
            "_id": market_status.get('region') + '_' + str(time_update),
            "market_type": market_status.get('market_type'),
            "region": market_status.get('region'),
            "primary_exchanges": market_status.get('primary_exchanges'),
            "local_open": market_status.get('local_open'),
            "local_close": market_status.get('local_close'),
            "current_status": market_status.get("current_status"),
            "notes": market_status.get("notes"),
            "time_update": time_update
        }

        mongodb.upsert_space_market(document)
        time.sleep(0.1)

def crawl_market_status(date: str = "2024-12-01"):
    time_update = parse_date_to_timestamp(date)
    
    list_market_status = get_market_status(date)
    
    load_all_market_status_to_db(list_market_status, time_update)