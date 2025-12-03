import psycopg2
from psycopg2.extensions import connection
from utils.config import GlobalConfig
from pprint import pprint
import os

def _init_env():
    GlobalConfig.ROOT = os.path.join(os.getcwd(), "..")
    GlobalConfig.MARKET_STATUS_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKET_STATUS_PATH)
    GlobalConfig.COMPANY_INFOS_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANY_INFOS_PATH)
    GlobalConfig.EXCHANGES_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.EXCHANGES_TABLE_PATH)
    GlobalConfig.COMPANIES_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANIES_TABLE_PATH)
    GlobalConfig.MARKETS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKETS_TABLE_PATH)
    GlobalConfig.MARKET_STATUS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKET_STATUS_TABLE_PATH)
    GlobalConfig.COMPANY_STATUS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANY_STATUS_TABLE_PATH)
    GlobalConfig.CONN = get_conn()

def get_conn(database: str="postgres",
             user: str="postgres",
             password: str="123",
             host="localhost",
             port=5432):
    try:
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

        print("Connection to the PostgreSQL established successfully.")
            
        return conn
    except Exception as e:
        print(f"Connection to the PostgreSQL encountered and error {e}.")
        return False

def test(sql: str):
    conn = GlobalConfig.CONN
    cursor = conn.cursor()
    cursor.execute(query=sql)

    conn.commit()
    print("Execute OK")
    res = cursor.fetchmany(size=5)
    pprint(res)

def delete_schema():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()
    cursor.execute("DROP SCHEMA datasource CASCADE")

    conn.commit()
    print("Execute OK")

def truncate(table: str, cascade: bool):
    conn = GlobalConfig.CONN
    cursor = conn.cursor()
    cursor.execute(query=f"""TRUNCATE TABLE datasource.{table} {"CASCADE" if cascade else ""}""")

    conn.commit()
    print("Execute OK")

def import_companies_table():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()
    cursor.execute(query=f"""COPY datasource.companies(company_ticker,
                                                        company_cik,
                                                        company_composite_figi,
                                                        company_market_locale,
                                                        company_share_class_figi,
                                                        company_asset_type,
                                                        company_name)
                            FROM '{GlobalConfig.COMPANIES_TABLE_PATH}'
                            DELIMITER ','
                            CSV HEADER;
                        """)
    conn.commit()
    print("Execute OK")

def import_markets_table():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()

    cursor.execute(query=f"""COPY datasource.markets(market_region,
                                                    market_type,
                                                    market_local_close,
                                                    market_local_open)
                            FROM '{GlobalConfig.MARKETS_TABLE_PATH}'
                            DELIMITER ','
                            CSV HEADER;
                        """)
    conn.commit()
    print("Execute OK")

def import_market_status_table():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()

    cursor.execute(query=f"""COPY datasource.market_status(market_status_region,
                                                            market_status_time_update,
                                                            market_status_current_status)
                            FROM '{GlobalConfig.MARKET_STATUS_TABLE_PATH}'
                            DELIMITER ','
                            CSV HEADER;
                        """)
    conn.commit()
    print("Execute OK")

def import_exchanges_table():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()
    cursor.execute(query=f"""COPY datasource.exchanges(exchange_mic,
                                                        exchange_region,
                                                        exchange_name)
                            FROM '{GlobalConfig.EXCHANGES_TABLE_PATH}'
                            DELIMITER ','
                            CSV HEADER;
                        """)
    conn.commit()
    print("Execute OK")

def import_company_status_table():
    conn = GlobalConfig.CONN
    cursor = conn.cursor()

    cursor.execute(query=f"""COPY datasource.company_status(company_status_ticker,
                                                            company_status_primary_exchange,
                                                            company_status_time_update,
                                                            company_status_type,
                                                            company_status_active)
                            FROM '{GlobalConfig.COMPANY_STATUS_TABLE_PATH}'
                            DELIMITER ','
                            CSV HEADER;
                        """)
    conn.commit()
    print("Execute OK")