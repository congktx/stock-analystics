import pyodbc
import pandas as pd


# conn = connect(connection_str=(
#         f"Server={'127.0.0.1,1433'};"
#         f"UID={'sa'};"
#         f"PWD={'Long2004@'};"
#         "Encrypt=no;"
#         "TrustServerCertificate=yes;"
#         )
#         )

# print(conn)

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=warehouse;"
    "UID=sa;"
    "PWD=Long2004@;"
    "TrustServerCertificate=yes;"
)

df = pd.read_sql("SELECT * FROM dbo.news_sentiment_processed", conn)
df.to_csv("news_sentiment_processed.csv", index=False, quoting=1)  # QUOTE_ALL

df = pd.read_sql("SELECT * FROM dbo.news_ticker_mapping", conn)
df.to_csv("news_ticker_mapping.csv", index=False, quoting=1)  # QUOTE_ALL

df = pd.read_sql("SELECT * FROM dbo.ticker_sentiment_daily", conn)
df.to_csv("ticker_sentiment_daily.csv", index=False, quoting=1)  # QUOTE_ALL
