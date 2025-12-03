from psycopg2.extensions import connection
from utils.config import GlobalConfig

def init_db():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()

    cursor.execute(query="CREATE SCHEMA datasource;")

    cursor.execute(query="""CREATE TABLE datasource.companies(
                                company_ticker VARCHAR(50) PRIMARY KEY,
                                company_cik VARCHAR(50),
                                company_composite_figi VARCHAR(50),
                                company_market_locale VARCHAR(50),
                                company_share_class_figi VARCHAR(50),
                                company_asset_type VARCHAR(50),
                                company_name VARCHAR(255)
                            );""")

    cursor.execute(query="""CREATE TABLE datasource.markets(
                                market_region VARCHAR(50) PRIMARY KEY,
                                market_type VARCHAR(50),
                                market_local_close VARCHAR(50),
                                market_local_open VARCHAR(50)
                            );""")

    cursor.execute(query="""CREATE TABLE datasource.market_status(
                                market_status_region VARCHAR(50) REFERENCES datasource.markets(market_region),
                                market_status_time_update INT4 NOT NULL,
                                market_status_current_status VARCHAR(50),
                                PRIMARY KEY (market_status_region, market_status_time_update)
                            );""")

    cursor.execute(query="""CREATE TABLE datasource.exchanges(
                                exchange_mic VARCHAR(50) PRIMARY KEY,
                                exchange_region VARCHAR(50) NOT NULL REFERENCES datasource.markets(market_region),
                                exchange_name VARCHAR(50)
                            );""")

    cursor.execute(query="""CREATE TABLE datasource.company_status(
                                company_status_ticker VARCHAR(50) REFERENCES datasource.companies(company_ticker),
                                company_status_primary_exchange VARCHAR(50) REFERENCES datasource.exchanges(exchange_mic),
                                company_status_time_update INT4,
                                company_status_type VARCHAR(50),
                                company_status_active VARCHAR(50),
                                PRIMARY KEY (company_status_ticker, company_status_time_update)
                            );""")
    conn.commit()
    print("Execute OK")