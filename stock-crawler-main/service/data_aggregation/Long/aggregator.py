import time
import os
import json
import requests
from pymongo import MongoClient, DESCENDING, ASCENDING

from database.mongodb import MongoDB

from utils.time_utils import timestamp_to_date, timestamp_to_YYYYMMDDTHH

from config import PolygonConfig, AssignedCompaniesConfig

mongodb = MongoDB()

MAX_FILE_SIZE_MB = 2


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
    
    mongodb.upsert_space_many_ohlc(list_documents)
    time.sleep(0.1)
    
class DataAggregator:
    def __init__(self, data_directory):
        self.data_directory = data_directory

    def size_in_mb(self, obj):
        json_str = json.dumps(obj)
        size_bytes = len(json_str.encode("utf-8"))
        return size_bytes / (1024 * 1024)

    def aggregate_ohlc_data(self):
        aggregated_data = []
        index_output_file = 1
        for i in range(1, 1410):
            file_path = os.path.join(self.data_directory + "/ohlc", f"{i}.json")
            size = self.size_in_mb(aggregated_data)
            print(f"Current aggregated data size: {size:.2f} MB")
            if size < MAX_FILE_SIZE_MB:
                try :
                    with open(file_path, 'r') as file:
                        print(f"Processing file: {file_path}")
                        data = json.load(file)
                        for item in data:
                            t = item["t"]
                            item["t"] = {
                                "$numberLong": t
                            }
                            item["_id"] = f"{item["ticker"]}_{t}"
                            aggregated_data.append(item)
                except FileNotFoundError:
                    print(f"File {file_path} not found. Skipping.")
                    continue
            else:
                with open(f"./aggregated_data/Lợi/ohlc/{index_output_file}.json", 'w') as file:
                    json.dump(aggregated_data, file, indent=4)
                index_output_file += 1
                aggregated_data.clear()
        return aggregated_data

    def aggregate_news_sentiment_data(self):
        aggregated_data = []
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.json'):
                file_path = os.path.join(self.data_directory, filename)
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    aggregated_data.extend(data)
        return aggregated_data
            
            
if __name__ == "__main__":
    aggregator = DataAggregator("./Lợi")
    aggregator.aggregate_ohlc_data()