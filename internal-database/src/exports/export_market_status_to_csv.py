import json, csv
from pprint import pprint
from utils.config import (mic_code, 
                          GlobalConfig)

def export_data_of_exchanges_table() -> None:
    header = ["exchange_mic", 
              "exchange_region", 
              "exchange_name"]

    with open(GlobalConfig.MARKET_STATUS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)
        with open(GlobalConfig.EXCHANGES_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

            for item in data:
                item = dict(item)
                region = item.get("region")
                primary_exchanges = list(map(str.strip ,str(item.get("primary_exchanges")).split(",")))
                for exchange in primary_exchanges:
                    exchange_mic = mic_code.get(exchange)
                    exchange_region = region
                    exchange_name = exchange

                    row = [exchange_mic, exchange_region, exchange_name]
                    csv_writer.writerow(row)

def export_data_of_markets_table() -> None:
    header = ["market_region",
              "market_type",
              "market_local_close", 
              "market_local_open"]
    
    with open(GlobalConfig.MARKET_STATUS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)

        with open(GlobalConfig.MARKETS_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

            for item in data:
                item = dict(item)

                market_region = item.get("region")
                market_type = item.get("market_type")
                market_local_close = item.get("local_close")
                market_local_open = item.get("local_open")

                row = [market_region, 
                       market_type,  
                       market_local_close, 
                       market_local_open]
                
                csv_writer.writerow(row)

def export_data_of_market_status_table() -> None:
    header = ["market_status_region",
              "market_status_time_update",
              "market_status_current_status"]
    
    with open(GlobalConfig.MARKET_STATUS_PATH, mode="r", encoding="utf-8") as original_data_file:
        data = json.load(original_data_file)

        with open(GlobalConfig.MARKET_STATUS_TABLE_PATH, mode="w", newline="", encoding="utf-8") as target_data_file:
            csv_writer = csv.writer(target_data_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(header)

            for item in data:
                item = dict(item)
                
                market_status_region = item.get("region")
                market_status_time_update = item.get("time_update")
                market_status_current_status = item.get("current_status")
                
                row = [market_status_region,
                       market_status_time_update,
                       market_status_current_status]
                
                csv_writer.writerow(row)