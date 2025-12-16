import time
import os
import json
import requests
from pymongo import MongoClient, DESCENDING, ASCENDING

from database.mongodb import MongoDB

from utils.time_utils import timestamp_to_date, timestamp_to_YYYYMMDDTHH

from config import PolygonConfig, AssignedCompaniesConfig

mongodb = MongoDB()
kafka_producer = get_producer()

def get_ohlc(ticker, from_timestamp, to_timestamp):
    from_date = timestamp_to_date(from_timestamp)
    to_date = timestamp_to_date(to_timestamp)
    url = f'https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/hour/{from_date}/{to_date}'
    
    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "adjusted": True,
        "sort": "asc",
        "limit": 50000,
        "apiKey": PolygonConfig.API_KEY
    }
    
    try:
        response = requests.get(url, params=params, headers=headers).json()

        next_url = response.get('next_url')
        
        if not response.get('results'):
            return response.get('errors'), next_url

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None
    
def ohlc_get_next_url(url):
    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers).json()
        
        next_url = response.get('next_url')
        
        if not response.get('results'):
            return response.get('errors'), next_url

        return response['results'], next_url
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], None

def load_all_ohlc_to_db(ticker, list_ohlc, time_update):
    list_documents = []
    for ohlc in list_ohlc:
        document = {
            "_id": ticker + '_' + str(ohlc.get('t')),
            "ticker": ticker,
            "t": ohlc.get('t'),
            "o": ohlc.get('o'),
            "h": ohlc.get('h'),
            'l': ohlc.get('l'),
            'c': ohlc.get('c'),
            'v': ohlc.get('v'),
            "time_update": time_update
        }

        list_documents.append(document)
    
    # Try to send to Kafka first
    if KafkaConfig.ENABLE_KAFKA:
        try:
            kafka_producer.send_batch(
                KafkaConfig.TOPIC_OHLC_DATA,
                list_documents,
                key_field='ticker'
            )
        except Exception as e:
            logger.error(f"Error sending OHLC to Kafka: {e}")
    
    # Fallback to MongoDB or use as primary storage
    if not KafkaConfig.ENABLE_KAFKA or KafkaConfig.FALLBACK_TO_MONGODB:
        mongodb.upsert_space_many_ohlc(list_documents)
    
    time.sleep(0.1)
        
def crawl_all_ohlc(from_timestamp, to_timestamp, time_update):
    timestamp = mongodb.find_last_timestamp(mongodb._company_infos)
    filter = {
        "time_update": timestamp
    }
    
    list_company_infos = list(mongodb.find_documents(mongodb._company_infos, filter))
    tickers = list(map(lambda x: x.get('ticker'), list_company_infos))

    last_ticker = mongodb.find_last_timestamp(mongodb._OHLC)
    is_start_crawl = False
    
    for ticker in tickers:
        if last_ticker == 'None' or last_ticker == ticker: is_start_crawl = True
        
        if not is_start_crawl: continue

        mongodb.upsert_last_completed_timestamp(mongodb._OHLC, ticker)
        
        list_ohlc, next_url = get_ohlc(ticker, from_timestamp, to_timestamp)
        load_all_ohlc_to_db(ticker, list_ohlc, time_update)
        
        time.sleep(12)
        
        while next_url:
            list_ohlc, next_url = ohlc_get_next_url(next_url)
            if isinstance(list_ohlc, list):
                load_all_ohlc_to_db(ticker, list_ohlc, time_update)
            time.sleep(12)
            
def crawl_assigned_companies_ohlc(from_timestamp, to_timestamp, time_update):
    # load division_of_labor.json from project root
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dov_path = os.path.join(root, "division_of_labor.json")
    
    try:
        with open(dov_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            tickers = data.get(AssignedCompaniesConfig.ASSIGNED_COMPANIES, [])
            last_index = data.get("ticker_index_finished", {}).get("Long", {}).get("ohlc", -1)
            print("tickers counted:", len(tickers))
    except Exception as e:
        print(f"Error loading division_of_labor.json: {e}")
        return
        tickers = []
    
    for index in range(last_index + 1, len(tickers)):
        ticker = tickers[index]
        earliest_ohlc = get_earliest_ohlc(ticker)
        if earliest_ohlc:
            easrliest_timestamp = earliest_ohlc.get('t') / 1000 + 3600  # plus one hour
            print(f"Start crawling OHLC for ticker {ticker} from {timestamp_to_YYYYMMDDTHH(from_timestamp)}")
            load_ohlc_to_db(ticker, from_timestamp, easrliest_timestamp, time_update)
        latest_ohlc = get_latest_ohlc(ticker)
        if latest_ohlc:
            latest_timestamp = latest_ohlc.get('t') / 1000 + 3600  # plus one hour
            print(f"Continue crawling OHLC for ticker {ticker} from {timestamp_to_YYYYMMDDTHH(latest_timestamp)}")
            load_ohlc_to_db(ticker, latest_timestamp, to_timestamp, time_update)
        else:
            print(f"Start crawling OHLC for ticker{ticker} from {timestamp_to_YYYYMMDDTHH(from_timestamp)}")
            load_ohlc_to_db(ticker, from_timestamp, to_timestamp, time_update)
        print(f"Completed crawling OHLC data for ticker {ticker}.")
        try:
            with open(dov_path, "r+", encoding="utf-8") as f:
                data = json.load(f)
                data["ticker_index_finished"]["Long"] = index - 1  # Save last index
                f.seek(0)
                json.dump(data, f, indent=4)
                f.truncate()
        except Exception as e:
            print(f"Error updating progress in division_of_labor.json: {e}")
    print("Completed crawling assigned companies' OHLC data.")
    return
            
def get_latest_ohlc(ticker=None):
    """Get the most recent OHLC record based on timestamp t"""
    filter_query = {}
    if ticker:
        filter_query["ticker"] = ticker
        
    result = mongodb._OHLC.find_one(
        filter=filter_query,
        sort=[("t", DESCENDING)]
    )
    return result

def get_earliest_ohlc(ticker=None):
    """Get the most recent OHLC record based on timestamp t"""
    filter_query = {}
    if ticker:
        filter_query["ticker"] = ticker
        
    result = mongodb._OHLC.find_one(
        filter=filter_query,
        sort=[("t", ASCENDING)]
    )
    return result

def load_ohlc_to_db(ticker, from_timestamp, to_timestamp, time_update):
    list_ohlc, next_url = get_ohlc(ticker, from_timestamp, to_timestamp)
    if not list_ohlc:
        print(f"No new OHLC data for ticker {ticker}")
        return
    load_all_ohlc_to_db(ticker, list_ohlc, time_update)
    print(f"Fetched until {timestamp_to_YYYYMMDDTHH(list_ohlc[-1].get('t') / 1000) if list_ohlc else 'N/A'}")
    time.sleep(12)
    
    while next_url:
        list_ohlc, next_url = ohlc_get_next_url(next_url)
        if not list_ohlc:
            print(f"No new OHLC data for ticker {ticker}")
            time.sleep(12)
            break
        load_all_ohlc_to_db(ticker, list_ohlc, time_update)
        print(f"Fetched until {timestamp_to_YYYYMMDDTHH(list_ohlc[-1].get('t') / 1000) if list_ohlc else 'N/A'}")
        time.sleep(12)
# def check_ohlc_exists(ticker, from_timestamp, to_timestamp, time_window_in_seconds=3600):
#     filter_query = {
#         "ticker": ticker
#     }
#     list_ohlc = mongodb._OHLC.find_one(filter=filter_query)
#     if not list_ohlc:
#         print(f"No OHLC data found for ticker {ticker}")
#     while from_timestamp <= to_timestamp:
            
#     return result is not None