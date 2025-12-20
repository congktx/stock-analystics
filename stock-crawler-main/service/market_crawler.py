import time
import logging

import requests

from utils.time_utils import round_timestamp

from database.mongodb import MongoDB

from config import AlphavantageConfig

from utils.parse_timestamp import parse_date_to_timestamp

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.kafka_producer import get_producer
from kafka_config import KafkaConfig

logger = logging.getLogger(__name__)
mongodb = MongoDB()
kafka_producer = get_producer()


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
        
        # Try to send to Kafka first
        if KafkaConfig.ENABLE_KAFKA:
            try:
                kafka_producer.send_market_status(document)
            except Exception as e:
                logger.error(f"Error sending market status to Kafka: {e}")
        
        # Fallback to MongoDB or use as primary storage
        if not KafkaConfig.ENABLE_KAFKA or KafkaConfig.FALLBACK_TO_MONGODB:
            mongodb.upsert_space_market(document)
        
        time.sleep(0.1)

def crawl_market_status(date: str = "2024-12-01"):
    time_update = parse_date_to_timestamp(date)
    
    list_market_status = get_market_status(date)
    
    load_all_market_status_to_db(list_market_status, time_update)