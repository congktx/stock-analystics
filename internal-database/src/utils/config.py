from psycopg2.extensions import connection
mic_code = {
    "NASDAQ": "XNAS",
    "NYSE": "XNYS",
    "AMEX": "XASE",
    "BATS": "BATS",
    "Toronto": "XTSE",
    "Toronto Ventures": "XTSX",
    "London": "XLON",
    "XETRA": "XETR",
    "Berlin": "XBER",
    "Frankfurt": "XFRA",
    "Munich": "XMUN",
    "Stuttgart": "XSTU",
    "Paris": "XPAR",
    "Barcelona": "XBAR",
    "Madrid": "XMAD",
    "Lisbon": "XLIS",
    "Tokyo": "XTKS",
    "NSE": "XNSE",
    "BSE": "XBOM",
    "Shanghai": "XSHG",
    "Shenzhen": "XSHE",
    "Hong Kong": "XHKG",
    "Sao Paolo": "BVMF",   
    "Mexico": "XMEX",
    "Johannesburg": "XJSE",
    "Global": "FOREX"      
}

mic_to_idx = {
    "XNAS": 1,
    "XNYS": 2
}

class GlobalConfig:
    MARKET_STATUS_PATH = "data/json/stock-analystics.market_status.json"
    COMPANY_INFOS_PATH = "data/json/stock-analystics.company_infos.json"
    EXCHANGES_TABLE_PATH = "data/csv/exchanges.csv"
    COMPANIES_TABLE_PATH = "data/csv/companies.csv"
    MARKETS_TABLE_PATH = "data/csv/markets.csv"
    MARKET_STATUS_TABLE_PATH = "data/csv/market_status.csv"
    COMPANY_STATUS_TABLE_PATH = "data/csv/company_status.csv"
    ROOT = None
    CONN: connection = None
