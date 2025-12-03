import sys
import logging

from pymongo import MongoClient

from dotenv import load_dotenv

from config import MongoDBConfig

from util.utils import parse_date_to_timestamp

load_dotenv()

logger = logging.getLogger("mongodb")


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        try:
            self.client = MongoClient(connection_url)
            self.db = self.client[MongoDBConfig.DATABASE]
        except Exception:
            logger.warning("Failed connecting to MongoDB Main")
            sys.exit()

        self._company_infos = self.get_collection('company-infos')
        self._market_status = self.get_collection('market-status')
        self._news_sentiment = self.get_collection('news-sentiment')
        self._OHLC = self.get_collection('OHLC')

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def get_company_infos(self, ticker: str | None, exchange: str | None, from_timestamp: int | None, to_timestamp: int | None, n_limit: int, n_page: int):
        query = {}
        if ticker:
            query.update({'ticker': ticker})

        if exchange:
            query.update({'primary_exchange': exchange})

        if from_timestamp or to_timestamp:
            query.update({'time_update': {}})

        if from_timestamp:
            query['time_update'].update({
                '$gte': from_timestamp
            })

        if to_timestamp:
            query['time_update'].update({
                '$lte': to_timestamp
            })

        num_document = self._company_infos.count_documents(query)

        page_count = num_document // n_limit

        if num_document % n_limit != 0:
            page_count += 1

        return {
            "page_id": n_page,
            "page_count": page_count,
            "documents": list(self._company_infos.find(query).skip((n_page - 1) * n_limit).limit(n_limit).sort({"_id": 1}))
        }

    def get_market_status(self):
        return list(self._market_status.find())

    def get_ohlc(self, ticker: str | None, from_timestamp: int | None, to_timestamp: int | None, n_limit: int, n_page: int):
        query = {}
        if ticker:
            query.update({'ticker': ticker})

        if from_timestamp or to_timestamp:
            query.update({'t': {}})

        if from_timestamp:
            query['t'].update({
                '$gte': from_timestamp
            })

        if to_timestamp:
            query['t'].update({
                '$lte': to_timestamp
            })

        num_document = self._OHLC.count_documents(query)

        page_count = num_document // n_limit

        if num_document % n_limit != 0:
            page_count += 1

        return {
            "page_id": n_page,
            "page_count": page_count,
            "documents": list(self._OHLC.find(query).skip((n_page - 1) * n_limit).limit(n_limit).sort({"_id": 1}))
        }

    def get_news_sentiment(self, ticker: str | None, from_date: str | None, to_date: str | None, n_limit: int, n_page: int):

        query = {}
        if ticker:
            query.update({
                "ticker_sentiment": {
                    "$regex": ticker,
                    "$options": "i"
                }
            })

        if from_date or to_date:
            query.update({'time_published': {}})

        if from_date:
            query['time_published'].update({
                '$gte': from_date
            })

        if to_date:
            query['time_published'].update({
                '$lte': from_date
            })

        num_document = self._news_sentiment.count_documents(query)

        page_count = num_document // n_limit

        if num_document % n_limit != 0:
            page_count += 1

        return {
            "page_id": n_page,
            "page_count": page_count,
            "documents": list(self._news_sentiment.find(query).skip((n_page - 1) * n_limit).limit(n_limit).sort({"_id": 1}))
        }
