"""
News Sentiment Processing Job for Apache Flink
Processes news sentiment data from Kafka and performs:
- Sentiment aggregation by ticker
- Time-windowed sentiment trends
- Anomaly detection for extreme sentiment changes
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Note: This is a Python-based Flink job template
# For production, consider using PyFlink or Java-based Flink for better performance

logger = logging.getLogger(__name__)


class NewsSentimentProcessor:
    """
    Process news sentiment data stream
    """
    
    def __init__(self):
        self.sentiment_window = {}  # ticker -> list of sentiments
        self.window_size_minutes = 60
    
    def process_news(self, news_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process individual news item and enrich with additional metrics
        
        Args:
            news_data: Raw news sentiment data
        
        Returns:
            Enriched news data
        """
        try:
            # Extract ticker information
            ticker_sentiments = news_data.get('ticker_sentiment', [])
            
            enriched_data = {
                **news_data,
                'processed_timestamp': datetime.now().isoformat(),
                'ticker_count': len(ticker_sentiments),
                'sentiment_metrics': {}
            }
            
            # Calculate aggregate metrics per ticker
            for ticker_data in ticker_sentiments:
                ticker = ticker_data.get('ticker')
                if not ticker:
                    continue
                
                sentiment_score = float(ticker_data.get('ticker_sentiment_score', 0))
                relevance_score = float(ticker_data.get('relevance_score', 0))
                
                # Weighted sentiment (sentiment * relevance)
                weighted_sentiment = sentiment_score * relevance_score
                
                enriched_data['sentiment_metrics'][ticker] = {
                    'sentiment_score': sentiment_score,
                    'relevance_score': relevance_score,
                    'weighted_sentiment': weighted_sentiment,
                    'sentiment_label': ticker_data.get('ticker_sentiment_label'),
                }
            
            # Add overall sentiment classification
            overall_score = news_data.get('overall_sentiment_score', 0)
            enriched_data['sentiment_category'] = self._classify_sentiment(overall_score)
            
            return enriched_data
            
        except Exception as e:
            logger.error(f"Error processing news: {e}")
            return news_data
    
    def _classify_sentiment(self, score: float) -> str:
        """Classify sentiment score into categories"""
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
    
    def aggregate_sentiment_by_ticker(
        self, 
        ticker: str, 
        news_items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Aggregate sentiment for a ticker over a time window
        
        Args:
            ticker: Stock ticker symbol
            news_items: List of news items mentioning the ticker
        
        Returns:
            Aggregated sentiment metrics
        """
        if not news_items:
            return {}
        
        sentiments = []
        relevances = []
        weighted_sentiments = []
        
        for news in news_items:
            metrics = news.get('sentiment_metrics', {}).get(ticker, {})
            if metrics:
                sentiments.append(metrics.get('sentiment_score', 0))
                relevances.append(metrics.get('relevance_score', 0))
                weighted_sentiments.append(metrics.get('weighted_sentiment', 0))
        
        if not sentiments:
            return {}
        
        # Calculate statistics
        avg_sentiment = sum(sentiments) / len(sentiments)
        avg_relevance = sum(relevances) / len(relevances)
        avg_weighted = sum(weighted_sentiments) / len(weighted_sentiments)
        
        # Sentiment volatility (standard deviation)
        sentiment_variance = sum((s - avg_sentiment) ** 2 for s in sentiments) / len(sentiments)
        sentiment_volatility = sentiment_variance ** 0.5
        
        # Count by sentiment category
        positive_count = sum(1 for s in sentiments if s > 0.15)
        negative_count = sum(1 for s in sentiments if s < -0.15)
        neutral_count = len(sentiments) - positive_count - negative_count
        
        return {
            'ticker': ticker,
            'news_count': len(news_items),
            'avg_sentiment': round(avg_sentiment, 4),
            'avg_relevance': round(avg_relevance, 4),
            'avg_weighted_sentiment': round(avg_weighted, 4),
            'sentiment_volatility': round(sentiment_volatility, 4),
            'positive_count': positive_count,
            'negative_count': negative_count,
            'neutral_count': neutral_count,
            'sentiment_distribution': {
                'positive_ratio': round(positive_count / len(sentiments), 2),
                'negative_ratio': round(negative_count / len(sentiments), 2),
                'neutral_ratio': round(neutral_count / len(sentiments), 2),
            },
            'window_start': news_items[0].get('time_published'),
            'window_end': news_items[-1].get('time_published'),
            'aggregated_at': datetime.now().isoformat()
        }
    
    def detect_sentiment_anomaly(
        self, 
        current_sentiment: float, 
        historical_sentiments: List[float],
        threshold: float = 2.0
    ) -> Dict[str, Any]:
        """
        Detect anomalies in sentiment using statistical methods
        
        Args:
            current_sentiment: Current sentiment score
            historical_sentiments: List of historical sentiment scores
            threshold: Number of standard deviations for anomaly detection
        
        Returns:
            Anomaly detection result
        """
        if len(historical_sentiments) < 10:
            return {'is_anomaly': False, 'reason': 'insufficient_history'}
        
        avg = sum(historical_sentiments) / len(historical_sentiments)
        variance = sum((s - avg) ** 2 for s in historical_sentiments) / len(historical_sentiments)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            return {'is_anomaly': False, 'reason': 'no_variance'}
        
        z_score = abs((current_sentiment - avg) / std_dev)
        is_anomaly = z_score > threshold
        
        return {
            'is_anomaly': is_anomaly,
            'z_score': round(z_score, 2),
            'threshold': threshold,
            'current_sentiment': current_sentiment,
            'avg_sentiment': round(avg, 4),
            'std_dev': round(std_dev, 4),
            'direction': 'spike' if current_sentiment > avg else 'drop'
        }


# PyFlink job template (requires pyflink package)
def create_news_processing_job():
    """
    Create Flink streaming job for news sentiment processing
    This is a template - requires PyFlink to be installed
    """
    
    print("""
    # PyFlink News Processing Job Template
    
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
    from pyflink.common.serialization import SimpleStringSchema
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    # Configure Kafka consumer
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'news-processor'
    }
    
    kafka_consumer = FlinkKafkaConsumer(
        topics='stock-news-sentiment',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Create data stream
    news_stream = env.add_source(kafka_consumer)
    
    # Process stream
    processor = NewsSentimentProcessor()
    
    processed_stream = news_stream.map(
        lambda x: processor.process_news(json.loads(x))
    )
    
    # Aggregate by ticker with tumbling window
    aggregated_stream = processed_stream \
        .key_by(lambda x: x['ticker']) \
        .window(TumblingEventTimeWindows.of(Time.minutes(60))) \
        .aggregate(SentimentAggregator())
    
    # Sink to MongoDB (custom sink)
    aggregated_stream.add_sink(MongoDBSink())
    
    # Execute job
    env.execute("News Sentiment Processing Job")
    """)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test processor
    processor = NewsSentimentProcessor()
    
    test_news = {
        'title': 'Apple announces record earnings',
        'overall_sentiment_score': 0.45,
        'ticker_sentiment': [
            {
                'ticker': 'AAPL',
                'ticker_sentiment_score': '0.5',
                'relevance_score': '0.9',
                'ticker_sentiment_label': 'Bullish'
            }
        ]
    }
    
    processed = processor.process_news(test_news)
    print("Processed news:", json.dumps(processed, indent=2))
    
    # Test aggregation
    news_items = [processed] * 5
    aggregated = processor.aggregate_sentiment_by_ticker('AAPL', news_items)
    print("\nAggregated sentiment:", json.dumps(aggregated, indent=2))
    
    # Test anomaly detection
    historical = [0.1, 0.15, 0.12, 0.18, 0.14, 0.16, 0.13, 0.17, 0.11, 0.15]
    anomaly = processor.detect_sentiment_anomaly(0.8, historical)
    print("\nAnomaly detection:", json.dumps(anomaly, indent=2))
