import json
import logging
from datetime import datetime
from typing import Dict, Any

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
    )
    from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy, Time
    from pyflink.datastream.functions import MapFunction, AggregateFunction, ProcessWindowFunction
    from pyflink.datastream.window import TumblingProcessingTimeWindows
    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False
    print("PyFlink not installed.")

from config.kafka_config import KafkaConfig, FlinkConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NewsSentimentEnrichmentFunction(MapFunction):
    """
    Enrich news sentiment
    """
    
    def map(self, value: str) -> str:
        try:
            data = json.loads(value)
            
            # Extract sentiment data
            ticker_sentiments = data.get('ticker_sentiment', [])
            overall_score = float(data.get('overall_sentiment_score', 0))
            overall_label = data.get('overall_sentiment_label', 'Unknown')
            
            # Analyze ticker sentiments
            tickers_info = []
            total_relevance = 0
            weighted_sentiment = 0
            
            for ts in ticker_sentiments:
                ticker = ts.get('ticker')
                sentiment_score = float(ts.get('ticker_sentiment_score', 0))
                relevance = float(ts.get('relevance_score', 0))
                
                tickers_info.append({
                    'ticker': ticker,
                    'sentiment_score': sentiment_score,
                    'sentiment_label': ts.get('ticker_sentiment_label'),
                    'relevance': relevance,
                    'weighted_score': sentiment_score * relevance
                })
                
                total_relevance += relevance
                weighted_sentiment += sentiment_score * relevance
            
            # Calculate aggregate metrics
            avg_weighted_sentiment = weighted_sentiment / total_relevance if total_relevance > 0 else 0
            
            enriched_data = {
                **data,
                'processed_timestamp': datetime.now().isoformat(),
                'processor': 'pyflink',
                'sentiment_analytics': {
                    'overall_score': overall_score,
                    'overall_label': overall_label,
                    'ticker_count': len(ticker_sentiments),
                    'avg_weighted_sentiment': round(avg_weighted_sentiment, 4),
                    'total_relevance': round(total_relevance, 2),
                    'tickers': tickers_info,
                    'sentiment_category': self._categorize_sentiment(overall_score)
                }
            }
            
            logger.info(
                f"Processed News: {data.get('title', 'No title')[:50]}... | "
                f"Sentiment: {overall_label} ({overall_score:.2f}) | "
                f"Tickers: {len(ticker_sentiments)}"
            )
            
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Error enriching news data: {e}")
            return value
    
    def _categorize_sentiment(self, score: float) -> str:
        """Categorize sentiment score"""
        if score >= 0.35:
            return 'very_positive'
        elif score >= 0.15:
            return 'positive'
        elif score >= -0.15:
            return 'neutral'
        elif score >= -0.35:
            return 'negative'
        else:
            return 'very_negative'


class SentimentByTickerKeySelector:
    """
    Key selector to group by ticker
    """
    
    def get_key(self, value: str) -> str:
        try:
            data = json.loads(value)
            # Get first ticker or 'UNKNOWN'
            ticker_sentiments = data.get('ticker_sentiment', [])
            if ticker_sentiments and len(ticker_sentiments) > 0:
                return ticker_sentiments[0].get('ticker', 'UNKNOWN')
            return 'UNKNOWN'
        except:
            return 'UNKNOWN'


class SentimentAggregator(AggregateFunction):
    """
    Aggregate sentiment scores for a ticker over time window
    """
    
    def create_accumulator(self):
        return {
            'count': 0,
            'sum_sentiment': 0.0,
            'sum_relevance': 0.0,
            'positive_count': 0,
            'negative_count': 0,
            'neutral_count': 0
        }
    
    def add(self, value: str, accumulator: Dict):
        try:
            data = json.loads(value)
            analytics = data.get('sentiment_analytics', {})
            
            overall_score = analytics.get('overall_score', 0)
            total_relevance = analytics.get('total_relevance', 0)
            
            accumulator['count'] += 1
            accumulator['sum_sentiment'] += overall_score
            accumulator['sum_relevance'] += total_relevance
            
            # Count sentiment types
            if overall_score > 0.15:
                accumulator['positive_count'] += 1
            elif overall_score < -0.15:
                accumulator['negative_count'] += 1
            else:
                accumulator['neutral_count'] += 1
                
        except Exception as e:
            logger.error(f"Error in aggregator add: {e}")
        
        return accumulator
    
    def get_result(self, accumulator: Dict) -> str:
        count = accumulator['count']
        if count == 0:
            return json.dumps({})
        
        result = {
            'news_count': count,
            'avg_sentiment': round(accumulator['sum_sentiment'] / count, 4),
            'total_relevance': round(accumulator['sum_relevance'], 2),
            'positive_ratio': round(accumulator['positive_count'] / count, 2),
            'negative_ratio': round(accumulator['negative_count'] / count, 2),
            'neutral_ratio': round(accumulator['neutral_count'] / count, 2),
            'aggregated_at': datetime.now().isoformat()
        }
        
        return json.dumps(result)
    
    def merge(self, acc_a: Dict, acc_b: Dict) -> Dict:
        return {
            'count': acc_a['count'] + acc_b['count'],
            'sum_sentiment': acc_a['sum_sentiment'] + acc_b['sum_sentiment'],
            'sum_relevance': acc_a['sum_relevance'] + acc_b['sum_relevance'],
            'positive_count': acc_a['positive_count'] + acc_b['positive_count'],
            'negative_count': acc_a['negative_count'] + acc_b['negative_count'],
            'neutral_count': acc_a['neutral_count'] + acc_b['neutral_count']
        }


def create_news_flink_job():
    
    if not PYFLINK_AVAILABLE:
        logger.error("PyFlink is not available")
        return None
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FlinkConfig.DEFAULT_PARALLELISM)
    env.enable_checkpointing(FlinkConfig.CHECKPOINT_INTERVAL_MS)
    
    logger.info("PyFlink News Sentiment Processing Job")
    logger.info(f"Kafka Bootstrap: {KafkaConfig.BOOTSTRAP_SERVERS}")
    logger.info(f"Input Topic: {KafkaConfig.TOPIC_NEWS_SENTIMENT}")
    logger.info(f"Parallelism: {FlinkConfig.DEFAULT_PARALLELISM}")
    
    # Configure Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS) \
        .set_topics(KafkaConfig.TOPIC_NEWS_SENTIMENT) \
        .set_group_id('flink-news-processor') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create data stream
    news_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "News Sentiment Kafka Source"
    )
    
    enriched_stream = news_stream \
        .map(NewsSentimentEnrichmentFunction()) \
        .name("Enrich News Sentiment")
    
    enriched_stream.print()
    
    # Configure Kafka sink
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("stock-news-processed")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    
    # Sink to Kafka
    enriched_stream.sink_to(kafka_sink).name("Kafka Sink - Processed News")
    
    return env


def run_news_job():
    env = create_news_flink_job()
    
    if env:
        logger.info("Starting News Sentiment Processing Job")
        
        try:
            # Execute the job
            env.execute("Stock News Sentiment Real-time Processing")
        except KeyboardInterrupt:
            logger.info("\nJob interrupted")
        except Exception as e:
            logger.error(f"Job execution error: {e}")
    else:
        logger.error("Failed to create Flink job")


if __name__ == "__main__":
    run_news_job()
