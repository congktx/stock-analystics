import requests
import logging

from config import AlphavantageConfig

from database.mongodb import MongoDB

from utils.utils import text_to_hash

from utils.time_utils import timestamp_to_YYYYMMDDTHHMM

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.kafka_producer import get_producer
from kafka_config import KafkaConfig

import time

logger = logging.getLogger(__name__)
mongodb = MongoDB()
kafka_producer = get_producer()

def get_news_sentiment(tickers, from_timestamp, to_timestamp):
    url = 'https://www.alphavantage.co/query'
    
    params = {
        "function": 'NEWS_SENTIMENT',
        "tickers": tickers,
        "apikey": AlphavantageConfig.API_KEY,
        "limit": 1000
    }
    
    if from_timestamp:
        params.update({
            "time_from": timestamp_to_YYYYMMDDTHHMM(from_timestamp),
        })
        
    if to_timestamp:
        params.update({
            "time_to": timestamp_to_YYYYMMDDTHHMM(to_timestamp),
        })
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers).json()
        
        # Kiểm tra lỗi từ API
        if response.get('Information'):
            # API trả về thông báo lỗi
            if 'rate limit' in response.get('Information', '').lower():
                print(f"⚠️ Rate limit: {response.get('Information')}")
            elif 'invalid' in response.get('Information', '').lower():
                # Ticker không hợp lệ, bỏ qua không in lỗi
                pass
            return []
        
        if not response.get('feed'):
            return []
        
        list_news = []
        for new in response.get('feed'):
            new.update({
                "sentiment_score_definition": response.get('sentiment_score_definition'),
                "relevance_score_definition": response.get('relevance_score_definition')
            })
            list_news.append(new)
        return list_news
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def load_all_news_sentiment_to_db(list_news, time_update):
    for news in list_news:
        document = {
            "_id": text_to_hash(news.get('title') + '_' + news.get('url')) + '_' + str(time_update),
            "title": news.get('title'),
            "url": news.get('url'),
            "time_published": news.get('time_published'),
            "authors": news.get('authors'),
            "summary": news.get('summary'),
            "source": news.get('source'),
            "source_domain": news.get('source_domain'),
            "topics": news.get('topics'),
            "overall_sentiment_score": news.get('overall_sentiment_score'),
            "overall_sentiment_label": news.get('overall_sentiment_label'),
            "ticker_sentiment": news.get('ticker_sentiment'),
            "sentiment_score_definition": news.get('sentiment_score_definition'),
            "relevance_score_definition": news.get('relevance_score_definition'),
            "time_update": time_update
        }
        
        # Try to send to Kafka first
        kafka_sent = False
        if KafkaConfig.ENABLE_KAFKA:
            try:
                # Extract ticker from ticker_sentiment for partitioning
                ticker = None
                if news.get('ticker_sentiment') and len(news.get('ticker_sentiment')) > 0:
                    ticker = news.get('ticker_sentiment')[0].get('ticker')
                
                kafka_sent = kafka_producer.send_news_sentiment({
                    **document,
                    'ticker': ticker
                })
            except Exception as e:
                logger.error(f"Error sending to Kafka: {e}")
        
        # Fallback to MongoDB or use as primary storage
        if not kafka_sent or KafkaConfig.FALLBACK_TO_MONGODB:
            mongodb.upsert_space_news(document)
        
def crawl_news_sentiment(from_timestamp, to_timestamp, time_update):
    timestamp = mongodb.find_last_timestamp(mongodb._company_infos)
    filter = {
        "time_update": timestamp
    }
    list_company_infos = list(mongodb.find_documets(mongodb._company_infos, filter))
    tickers = list(map(lambda x: x.get('ticker'), list_company_infos))

    for ticker in tickers:
        list_news = get_news_sentiment(ticker, from_timestamp, to_timestamp)
        print(ticker, len(list_news))
        load_all_news_sentiment_to_db(list_news, time_update)    
        
        time.sleep(12)