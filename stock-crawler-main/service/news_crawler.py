import time

import json
import os
import requests
import logging

from config import AlphavantageConfig, AssignedCompaniesConfig

from database.mongodb import MongoDB

from utils.utils import text_to_hash

from utils.time_utils import timestamp_to_YYYYMMDDTHHMM

from utils.json_file import load_json_file, save_json_file

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
            if response.get('Information') and "rate limit" in response.get('Information'):
                return "rate limit"
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
    list_document = []
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
        list_document.append(document)
    
    mongodb.upsert_space_many_news(list_document)
    time.sleep(0.1)
        
def crawl_news_sentiment(from_timestamp, to_timestamp, time_update):
    timestamp = mongodb.find_last_timestamp(mongodb._company_infos)
    filter = {
        "time_update": time_update
    }
    list_company_infos = list(mongodb.find_documents(mongodb._company_infos, filter))
    tickers = list(map(lambda x: x.get('ticker'), list_company_infos))
    # dict = load_json_file('./tmp/division_of_labor.json')
    # tickers = dict.get('Thinh')
    # last_request = 'LPAA'
    # is_start_crawl = False
    for ticker in tickers:
        # if ticker == last_request:
        #     is_start_crawl = True
        # if not is_start_crawl:
        #     continue
        
        list_news = get_news_sentiment(ticker, from_timestamp, to_timestamp)
        print(ticker, len(list_news))
        if list_news == "rate limit": 
            print(f"rate limit at {ticker}")
            break
        load_all_news_sentiment_to_db(list_news, time_update)
        time.sleep(5)