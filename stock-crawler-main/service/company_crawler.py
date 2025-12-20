import requests

import time

from utils.time_utils import round_timestamp

from database.mongodb import MongoDB

from config import PolygonConfig

from utils.parse_timestamp import parse_date_to_timestamp

mongodb = MongoDB()

def get_company_infos(date: str = "2024-12-01", exchange = 'XNAS'):
    params = {
        "apiKey": PolygonConfig.API_KEY,
        "market": "stocks",
        "exchange": exchange,
        "active": "true",
        "order": "asc",
        "limit": "100",
        "sort": "ticker",
        "date": date,
    }

    headers = {
        "Content-Type": "application/json"
    }

    url = 'https://api.polygon.io/v3/reference/tickers'

    try:
        response = requests.get(url, params=params, headers=headers).json()
        next_url = response.get('next_url')
        if next_url:
            next_url = next_url.split('cursor=')[-1]
        if not response.get('results'):
            return response.get('errors')

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None


def polygon_get_next_url(cursor: str):
    params = {
        "cursor": cursor,
        "apiKey": PolygonConfig.API_KEY
    }

    headers = {
        "Content-Type": "application/json"
    }

    url = 'https://api.polygon.io/v3/reference/tickers'

    try:
        response = requests.get(url, params=params, headers=headers).json()
        next_url = response.get('next_url')

        if next_url:
            next_url = next_url.split('cursor=')[-1]

        if not response.get('results'):
            return response.get('errors')

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None

def load_all_company_infos_to_db(list_company_infos, time_update):
    for company_infos in list_company_infos:
        document = {
            "_id": company_infos.get('ticker') + '_' + str(time_update),
            "ticker": company_infos.get('ticker'),
            "name": company_infos.get('name'),
            "market": company_infos.get('market'),
            "locale": company_infos.get('locale'),
            "primary_exchange": company_infos.get('primary_exchange'),
            "type": company_infos.get('type'),
            "active": company_infos.get('active'),
            "currency_name": company_infos.get('currency_name'),
            "cik": company_infos.get('cik'),
            "composite_figi": company_infos.get('composite_figi'),
            "share_class_figi": company_infos.get('share_class_figi'),
            "time_update": time_update
        }
        
        mongodb.upsert_space_company(document)
        time.sleep(0.1)

def crawl_all_company(date: str = "2024-12-01", list_exchage = ['XNAS', 'XNYS']):
    time_update = parse_date_to_timestamp(date)
    
    for exchange in list_exchage:
        list_company_infos, cursor = get_company_infos(date, exchange)
        load_all_company_infos_to_db(list_company_infos, time_update)
        time.sleep(12)
        
        while cursor: 
            list_company_infos, cursor = polygon_get_next_url(cursor)
            load_all_company_infos_to_db(list_company_infos, time_update)
            time.sleep(12)
    
    mongodb.upsert_last_completed_timestamp(mongodb._company_infos, time_update)
    
    
    