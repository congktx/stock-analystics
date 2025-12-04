try:
    import psycopg2
    from psycopg2.extensions import connection
except ImportError:
    psycopg2 = None
    connection = None

from utils.config import GlobalConfig
from pprint import pprint
import os

def _init_env():
    # Use the directory of this file (utils.py) as the reference point
    # utils.py is in src/utils, so we go up two levels to get to the project root (if that's the intention)
    # However, based on the original code: GlobalConfig.ROOT = os.path.join(os.getcwd(), "..")
    # It seems the intention was to run from src/ and have ROOT be the parent of src/
    # Let's assume the structure is:
    # project/
    #   internal-database/
    #     src/
    #       utils/
    #         utils.py
    #       main.py
    #     data/
    
    # If we want ROOT to be 'internal-database', and we are in 'internal-database/src/utils/utils.py'
    # Then we need to go up 2 levels to 'src', then 1 level to 'internal-database'
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    project_root = os.path.dirname(src_dir)
    
    GlobalConfig.ROOT = project_root
    
    GlobalConfig.MARKET_STATUS_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKET_STATUS_PATH)
    GlobalConfig.COMPANY_INFOS_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANY_INFOS_PATH)
    GlobalConfig.EXCHANGES_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.EXCHANGES_TABLE_PATH)
    GlobalConfig.COMPANIES_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANIES_TABLE_PATH)
    GlobalConfig.MARKETS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKETS_TABLE_PATH)
    GlobalConfig.MARKET_STATUS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.MARKET_STATUS_TABLE_PATH)
    GlobalConfig.COMPANY_STATUS_TABLE_PATH = os.path.join(GlobalConfig.ROOT, GlobalConfig.COMPANY_STATUS_TABLE_PATH)
    
    # Ensure directories exist
    for path in [
        GlobalConfig.MARKET_STATUS_PATH,
        GlobalConfig.COMPANY_INFOS_PATH,
        GlobalConfig.EXCHANGES_TABLE_PATH,
        GlobalConfig.COMPANIES_TABLE_PATH,
        GlobalConfig.MARKETS_TABLE_PATH,
        GlobalConfig.MARKET_STATUS_TABLE_PATH,
        GlobalConfig.COMPANY_STATUS_TABLE_PATH
    ]:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    
    _generate_seed_data()

    GlobalConfig.CONN = get_conn()
    if GlobalConfig.CONN is None:
        print("CRITICAL ERROR: Failed to establish database connection. Exiting.")
        # We might want to raise an exception or exit here, but for now just printing.
        # Ideally, the caller should handle this.

def _generate_seed_data():
    """Generates default seed data for CSV files if they are missing."""
    
    # Markets
    if not os.path.exists(GlobalConfig.MARKETS_TABLE_PATH):
        print(f"Generating seed data for {GlobalConfig.MARKETS_TABLE_PATH}...")
        with open(GlobalConfig.MARKETS_TABLE_PATH, 'w') as f:
            f.write("market_region,market_type,market_local_close,market_local_open\n")
            f.write("US,Stock,16:00,09:30\n")
            f.write("VN,Stock,15:00,09:00\n")
            f.write("JP,Stock,15:00,09:00\n")
            f.write("UK,Stock,16:30,08:00\n")
            f.write("DE,Stock,17:30,09:00\n")

    # Exchanges
    if not os.path.exists(GlobalConfig.EXCHANGES_TABLE_PATH):
        print(f"Generating seed data for {GlobalConfig.EXCHANGES_TABLE_PATH}...")
        with open(GlobalConfig.EXCHANGES_TABLE_PATH, 'w') as f:
            f.write("exchange_mic,exchange_region,exchange_name\n")
            f.write("XNYS,US,New York Stock Exchange\n")
            f.write("XNAS,US,NASDAQ\n")
            f.write("XSTC,VN,Ho Chi Minh City Stock Exchange\n")
            f.write("XHNX,VN,Hanoi Stock Exchange\n")
            f.write("XTKS,JP,Tokyo Stock Exchange\n")
            f.write("XLON,UK,London Stock Exchange\n")
            f.write("XFRA,DE,Frankfurt Stock Exchange\n")

    # Market Status
    if not os.path.exists(GlobalConfig.MARKET_STATUS_TABLE_PATH):
        print(f"Generating seed data for {GlobalConfig.MARKET_STATUS_TABLE_PATH}...")
        with open(GlobalConfig.MARKET_STATUS_TABLE_PATH, 'w') as f:
            f.write("market_status_region,market_status_time_update,market_status_current_status\n")
            f.write("US,1700000000,Open\n")
            f.write("VN,1700000000,Closed\n")


def get_conn():
    if psycopg2 is None:
        print("Warning: psycopg2 module not found. Database connection will not be established.")
        return None
    try:
        conn = psycopg2.connect(
            database=GlobalConfig.DB_NAME,
            user=GlobalConfig.DB_USER,
            password=GlobalConfig.DB_PASSWORD,
            host=GlobalConfig.DB_HOST,
            port=GlobalConfig.DB_PORT
        )

        print("Connection to the PostgreSQL established successfully.")
            
        return conn
    except Exception as e:
        print(f"Connection to the PostgreSQL encountered an error: {e}.")
        return None

def test(sql: str):
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()
    cursor.execute(query=sql)

    conn.commit()
    print("Execute OK")
    res = cursor.fetchmany(size=5)
    pprint(res)

def delete_schema():
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS datasource CASCADE")

    conn.commit()
    print("Execute OK")

def truncate(table: str, cascade: bool):
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()
    cursor.execute(query=f"""TRUNCATE TABLE datasource.{table} {"CASCADE" if cascade else ""}""")

    conn.commit()
    print("Execute OK")

def import_companies_table():
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()
    with open(GlobalConfig.COMPANIES_TABLE_PATH, 'r') as f:
        cursor.copy_expert(sql="""COPY datasource.companies(company_ticker,
                                                            company_cik,
                                                            company_composite_figi,
                                                            company_market_locale,
                                                            company_share_class_figi,
                                                            company_asset_type,
                                                            company_name)
                                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')""", file=f)
    conn.commit()
    print("Execute OK")

def import_markets_table():
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()

    with open(GlobalConfig.MARKETS_TABLE_PATH, 'r') as f:
        cursor.copy_expert(sql="""COPY datasource.markets(market_region,
                                                        market_type,
                                                        market_local_close,
                                                        market_local_open)
                                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')""", file=f)
    conn.commit()
    print("Execute OK")

def import_market_status_table():
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()

    with open(GlobalConfig.MARKET_STATUS_TABLE_PATH, 'r') as f:
        cursor.copy_expert(sql="""COPY datasource.market_status(market_status_region,
                                                                market_status_time_update,
                                                                market_status_current_status)
                                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')""", file=f)
    conn.commit()
    print("Execute OK")

def import_exchanges_table():
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()
    with open(GlobalConfig.EXCHANGES_TABLE_PATH, 'r') as f:
        cursor.copy_expert(sql="""COPY datasource.exchanges(exchange_mic,
                                                            exchange_region,
                                                            exchange_name)
                                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')""", file=f)
    conn.commit()
    print("Execute OK")

def import_company_status_table():
    conn = GlobalConfig.CONN
    if conn is None:
        print("Database connection is not available.")
        return
    cursor = conn.cursor()

    with open(GlobalConfig.COMPANY_STATUS_TABLE_PATH, 'r') as f:
        cursor.copy_expert(sql="""COPY datasource.company_status(company_status_ticker,
                                                                company_status_primary_exchange,
                                                                company_status_time_update,
                                                                company_status_type,
                                                                company_status_active)
                                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')""", file=f)
    conn.commit()
    print("Execute OK")