import threading
from tqdm import tqdm

from service import *

if __name__ == '__main__':
    # crawl_all_company(list_exchage=['XNAS', 'XNYS'])
    crawl_news_sentiment(1704067200, 1761969690, 1704067200)
    # crawl_all_ohlc(1733011200, 1745971200, 1745971200)