# import json

# input_file = 'company_infos_4.json'
# output_file = 'company_infos_fixed_4.json'

# try:
#     with open(input_file, 'r', encoding='utf-8') as f:
#         raw_string = json.load(f)

#         if isinstance(raw_string, str):
#             real_data = json.loads(raw_string)
#         else:
#             real_data = raw_string

#     with open(output_file, 'w', encoding='utf-8') as f:
#         json.dump(real_data, f, indent=4, ensure_ascii=False)

#     print(f"Đã sửa xong! Kiểm tra file: {output_file}")

# except Exception as e:
#     print(f"Có lỗi xảy ra: {e}")

import time

import json

import requests

from database.mongodb import MongoDB

from config import PolygonConfig

def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

    return {}


def fetch_company_infos(ticker: str):
    params = {
        "apiKey": PolygonConfig.API_KEY,
        "date": ticker.get('date')
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    url = f'https://api.massive.com/v3/reference/tickers/{ticker.get('ticker')}'
    
    try:
        response = requests.get(url, params=params, headers=headers).json()

        if not response.get('results'):
            return response.get('errors')

        return response['results']
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

mongodb = MongoDB()

ticker = load_json_file('tickers.json')

for _ in range(1, 5):
    input_file = f'company_infos_fixed_{_}.json'
    data = load_json_file(input_file)
    for index, company_infos in enumerate(data):
        if not company_infos:
            print(f'Having null data at {index}', company_infos)

    for batch in range(0, len(data), 10):
        list_company_infos = []
        for index in range(batch, min(batch + 10, len(data))):
            company_infos = data[index]
            if not data[index]:
                company_infos = fetch_company_infos(ticker[(_ -1) * 1252 + index])
                time.sleep(12)
            list_company_infos.append(company_infos)

        mongodb.upsert_space_many_more_company_infos(list_company_infos)
        time.sleep(1)
