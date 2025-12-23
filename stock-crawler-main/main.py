import threading
from tqdm import tqdm

from service import *

if __name__ == '__main__':
    for year in [2024, 2025]:
        for month in range(1, 13):
            while True:
                try:
                    if year == 2025 and month > 10:
                        break
                    month_str = str(month)
                    if month < 10:
                        month_str = '0' + month_str

                    last_day = 31
                    if month in [4,6,9,11]:
                        last_day = 30
                    if month == 2:
                        last_day = 28
                    
                    print(f"{year}-{month_str}-{last_day}")

                    crawl_all_company(
                        date=f"{year}-{month_str}-{last_day}", list_exchage=['XNAS', 'XNYS'])
                    # crawl_news_sentiment(1704067200, 1761969690, 1704067200)
                    # crawl_all_ohlc(1704067200, 1761969690, 1704067200)
                    break
                except Exception as e:
                    print(e)
                    time.sleep(10)
    print("Finish")
