import os
from dotenv import load_dotenv

load_dotenv()


class MSSQLConfig:
    # CONNECTION_URL = (
    # f"Server={os.environ.get('MSSQL-URL') or 'localhost,1433'};"
    # f"Database={os.environ.get('MSSQL-DB') or 'FlightAnalytics'};"
    # "UID=sa;"
    # f"PWD={os.environ.get('MSSQL-PASSWORD') or 'Long2004@'};"
    # "TrustServerCertificate=yes;"
    # )
    CONNECTION_URL = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={os.getenv('MSSQL_URL', 'localhost,1433')};"
        f"Database={os.getenv('MSSQL_DB', 'FlightAnalytics')};"
        f"UID={os.getenv('MSSQL_USER', 'sa')};"
        f"PWD={os.getenv('MSSQL_PASSWORD', 'Long2004@')};"
        "TrustServerCertificate=yes;"
    )
    DATABASE = os.environ.get("MSSQL-DB") or "test"

# class PolygonConfig:
#     API_KEY = os.environ.get('POLYGON_API_KEY') or "abc"

# class AlphavantageConfig:
#     API_KEY = os.environ.get('ALPHAVANTAGE_API_KEY') or "abc"
    
# class AssignedCompaniesConfig:
#     ASSIGNED_COMPANIES = os.environ.get('ASSIGNED_COMPANIES') or "abc"

if __name__ == "__main__":
    print(MSSQLConfig.CONNECTION_URL)