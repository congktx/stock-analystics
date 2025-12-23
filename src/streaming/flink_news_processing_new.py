from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Configuration
import json
from datetime import datetime

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_INPUT_TOPIC = "raw-news-new"
KAFKA_OUTPUT_TOPIC_MAPPING = "news-ticker-mapping"
KAFKA_OUTPUT_TOPIC_DAILY = "ticker-sentiment-daily"

# Path to JAR files
JAR_FILES = [
    "file:///f:/stock-analystics/libs/flink-connector-kafka-3.0.2-1.18.jar",
    "file:///f:/stock-analystics/libs/flink-sql-connector-kafka-3.1.0-1.18.jar",
    "file:///f:/stock-analystics/libs/kafka-clients-3.2.3.jar"
]

def process_news(news):
    """Process raw news data and add sentiment analysis."""
    news_data = json.loads(news)

    # Example sentiment analysis (replace with actual logic)
    sentiment_score = len(news_data.get("title", "")) % 10 / 10.0
    sentiment_label = "Positive" if sentiment_score > 0.5 else "Negative"

    # Format data for `news_sentiment_processed`
    processed_news = {
        "message_id": news_data.get("message_id"),
        "source_id": news_data.get("source_id"),
        "title": news_data.get("title"),
        "url": news_data.get("url"),
        "time_published": news_data.get("time_published"),
        "summary": news_data.get("summary"),
        "source_name": news_data.get("source_name"),
        "overall_sentiment_score": sentiment_score,
        "overall_sentiment_label": sentiment_label,
        "processed_at": datetime.utcnow().isoformat()
    }

    # Example ticker mapping (replace with actual logic)
    ticker_mapping = {
        "news_id": news_data.get("message_id"),
        "ticker": "AAPL",  # Example ticker
        "relevance_score": 0.9,  # Example relevance score
        "ticker_sentiment_score": sentiment_score,
        "ticker_sentiment_label": sentiment_label,
        "created_at": datetime.utcnow().isoformat()
    }

    # Example daily aggregation (replace with actual logic)
    daily_aggregation = {
        "ticker": "AAPL",  # Example ticker
        "date": datetime.utcnow().date().isoformat(),
        "news_count": 1,
        "avg_sentiment_score": sentiment_score,
        "bullish_count": 1 if sentiment_label == "Positive" else 0,
        "bearish_count": 1 if sentiment_label == "Negative" else 0,
        "neutral_count": 1 if sentiment_label == "Neutral" else 0,
        "top_sentiment_score": sentiment_score,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }

    return json.dumps({
        "processed_news": processed_news,
        "ticker_mapping": ticker_mapping,
        "daily_aggregation": daily_aggregation
    })

def main():
    # Create a configuration object and set JAR files
    config = Configuration()
    config.set_string("pipeline.jars", ";".join(JAR_FILES))

    # Initialize the StreamExecutionEnvironment with the configuration
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)

    # Kafka Consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_INPUT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": KAFKA_BROKER}
    )

    # Kafka Producers
    kafka_producer_mapping = FlinkKafkaProducer(
        topic=KAFKA_OUTPUT_TOPIC_MAPPING,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BROKER}
    )

    kafka_producer_daily = FlinkKafkaProducer(
        topic=KAFKA_OUTPUT_TOPIC_DAILY,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BROKER}
    )

    # Define the processing pipeline
    processed_stream = env.add_source(kafka_consumer) \
        .map(process_news, output_type=Types.STRING())

    processed_stream.add_sink(kafka_producer_mapping)
    processed_stream.add_sink(kafka_producer_daily)

    # Execute the Flink job
    env.execute("News Processing Job - Full Pipeline")

if __name__ == "__main__":
    main()