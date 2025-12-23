import json
import pyodbc
from kafka import KafkaConsumer

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw-sql-data"

# SQL Server Configuration
SQL_SERVER = "192.168.0.108"
SQL_DATABASE = "Streaming"
SQL_USERNAME = "sa"
SQL_PASSWORD = "YourStrong@Password123"
SQL_TABLE_1 = "news_sentiment_processed"
SQL_TABLE_2 = "news_ticker_mapping"
SQL_TABLE_3 = "ticker_sentiment_daily"


def insert_into_sql_server_2(news_data):
    """Insert processed news data into SQL Server."""
    connection = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD}"
    )
    cursor = connection.cursor()

    insert_query = f"""
    INSERT INTO {SQL_TABLE_2} (
        news_id, ticker, relevance_score, ticker_sentiment_score, ticker_sentiment_label, created_at
    ) VALUES (?, ?, ?, ?, ?, GETUTCDATE())
    """

    for news in news_data:
        news_json = json.loads(news.value.decode('utf-8'))
        cursor.execute(insert_query, (
            news_json.get("message_id"),
            news_json.get("source_id"),
            news_json.get("title"),
            news_json.get("url"),
            news_json.get("time_published"),
            news_json.get("summary"),
            news_json.get("source_name"),
            news_json.get("overall_sentiment_score"),
            news_json.get("overall_sentiment_label")
        ))

    connection.commit()
    cursor.close()
    connection.close()


def insert_into_sql_server_1(news_data):
    """Insert processed news data into SQL Server."""
    connection = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD}"
    )
    cursor = connection.cursor()

    insert_query = f"""
    INSERT INTO {SQL_TABLE_1} (
        message_id, source_id, title, url, time_published, summary, source_name,
        overall_sentiment_score, overall_sentiment_label, processed_at, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE())
    """

    for news in news_data:
        news_json = json.loads(news.value.decode('utf-8'))
        cursor.execute(insert_query, (
            news_json.get("message_id"),
            news_json.get("source_id"),
            news_json.get("title"),
            news_json.get("url"),
            news_json.get("time_published"),
            news_json.get("summary"),
            news_json.get("source_name"),
            news_json.get("overall_sentiment_score"),
            news_json.get("overall_sentiment_label")
        ))

    connection.commit()
    cursor.close()
    connection.close()


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        group_id='sql-writer',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("Listening to Kafka topic and inserting into SQL Server...")
    for message in consumer:
        insert_into_sql_server_1([message])

if __name__ == "__main__":
    main()