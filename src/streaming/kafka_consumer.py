import json
import logging
import signal
import sys
from typing import Dict, Any, Optional, Callable
from datetime import datetime

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("kafka-python not installed.")

from config.kafka_config import KafkaConfig
from src.storage.mongodb import MongoDB

logger = logging.getLogger(__name__)


class StockDataConsumer:
    """
    Kafka consumer for stock data streams
    """
    
    def __init__(self, topic: str, group_id: str, processor: Optional[Callable] = None):
        self.topic = topic
        self.group_id = group_id
        self.processor = processor
        self.running = True
        self.processed_count = 0
        self.error_count = 0
        self.consumer = None
        
        # MongoDB connection
        self.mongodb = MongoDB()
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        if KAFKA_AVAILABLE:
            self._initialize_consumer()
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                **KafkaConfig.CONSUMER_CONFIG,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info(f"✓ Consumer initialized for topic: {self.topic}")
        except Exception as e:
            logger.error(f"✗ Failed to initialize consumer: {e}")
            self.consumer = None
    
    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown"""
        logger.info("\n🛑 Shutting down consumer...")
        self.running = False
        if self.consumer:
            self.consumer.close()
        logger.info(f"📊 Stats - Processed: {self.processed_count}, Errors: {self.error_count}")
    
    def process_ohlc_message(self, data: Dict[str, Any]) -> bool:
        try:
            # Add processing metadata
            data['processed_at'] = datetime.utcnow().isoformat()
            data['processor'] = 'kafka_consumer'
            
            # Calculate technical indicators
            close_price = float(data.get('c', 0))
            high_price = float(data.get('h', 0))
            low_price = float(data.get('l', 0))
            open_price = float(data.get('o', 0))
            volume = float(data.get('v', 0))
            
            # Price metrics
            price_range = high_price - low_price
            price_change = close_price - open_price
            
            data['technical_indicators'] = {
                'price_range': round(price_range, 2),
                'price_range_pct': round((price_range / close_price * 100), 2) if close_price > 0 else 0,
                'price_change': round(price_change, 2),
                'price_change_pct': round((price_change / open_price * 100), 2) if open_price > 0 else 0,
                'volume': volume,
                'high_low_ratio': round(high_price / low_price, 4) if low_price > 0 else 0
            }
            
            # Save to MongoDB processed collection
            collection = self.mongodb.get_collection('OHLC_processed')
            collection.update_one(
                {'_id': data['_id']},
                {'$set': data},
                upsert=True
            )
            
            logger.info(
                f"✓ OHLC: {data.get('ticker')} | "
                f"Close: ${close_price:.2f} | "
                f"Change: {data['technical_indicators']['price_change_pct']:.2f}%"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error processing OHLC: {e}")
            return False
    
    def process_news_message(self, data: Dict[str, Any]) -> bool:

        try:
            # Add processing metadata
            data['processed_at'] = datetime.utcnow().isoformat()
            data['processor'] = 'kafka_consumer'
            
            # Extract and summarize sentiment
            ticker_sentiments = data.get('ticker_sentiment', [])
            
            sentiment_summary = {
                'ticker_count': len(ticker_sentiments),
                'overall_sentiment': data.get('overall_sentiment_label', 'Unknown'),
                'overall_score': float(data.get('overall_sentiment_score', 0)),
                'tickers': []
            }
            
            for ticker_data in ticker_sentiments:
                sentiment_summary['tickers'].append({
                    'ticker': ticker_data.get('ticker'),
                    'sentiment': ticker_data.get('ticker_sentiment_label'),
                    'score': float(ticker_data.get('ticker_sentiment_score', 0)),
                    'relevance': float(ticker_data.get('relevance_score', 0))
                })
            
            data['sentiment_summary'] = sentiment_summary
            
            # Save to MongoDB
            collection = self.mongodb.get_collection('news_sentiment_processed')
            collection.update_one(
                {'_id': data['_id']},
                {'$set': data},
                upsert=True
            )
            
            title = data.get('title', 'No title')[:50]
            logger.info(
                f"✓ News: {title}... | "
                f"Sentiment: {sentiment_summary['overall_sentiment']} | "
                f"Tickers: {len(ticker_sentiments)}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error processing news: {e}")
            return False
    
    def process_company_message(self, data: Dict[str, Any]) -> bool:
        """Process company info data"""
        try:
            data['processed_at'] = datetime.utcnow().isoformat()
            
            collection = self.mongodb.get_collection('company_info_processed')
            collection.update_one(
                {'_id': data['_id']},
                {'$set': data},
                upsert=True
            )
            
            logger.info(f"✓ Company: {data.get('ticker')} - {data.get('name')}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error processing company: {e}")
            return False
    
    def process_market_message(self, data: Dict[str, Any]) -> bool:
        """Process market status data"""
        try:
            data['processed_at'] = datetime.utcnow().isoformat()
            
            collection = self.mongodb.get_collection('market_status_processed')
            collection.update_one(
                {'_id': data['_id']},
                {'$set': data},
                upsert=True
            )
            
            logger.info(
                f"✓ Market: {data.get('region')} - "
                f"Status: {data.get('current_status')}"
            )
            return True
            
        except Exception as e:
            logger.error(f"✗ Error processing market: {e}")
            return False
    
    def start(self):
        """Start consuming messages"""
        if not KAFKA_AVAILABLE or not self.consumer:
            logger.error("Kafka not available or consumer not initialized")
            return
        
        logger.info(f"Starting consumer for topic: {self.topic}")
        logger.info(f"Bootstrap servers: {KafkaConfig.BOOTSTRAP_SERVERS}")
        logger.info(f"Consumer group: {self.group_id}")
        logger.info("Waiting for messages...\n")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    data = message.value
                    
                    # Use custom processor if provided, otherwise use default
                    if self.processor:
                        success = self.processor(data)
                    else:
                        success = self._default_processor(data)
                    
                    if success:
                        self.processed_count += 1
                        # Commit offset after successful processing
                        self.consumer.commit()
                    else:
                        self.error_count += 1
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.error_count += 1
                    
        except KeyboardInterrupt:
            logger.info("\nInterrupted")
        finally:
            self.shutdown()
    
    def _default_processor(self, data: Dict[str, Any]) -> bool:
        """Default message processor based on topic"""
        if KafkaConfig.TOPIC_OHLC_DATA in self.topic:
            return self.process_ohlc_message(data)
        elif KafkaConfig.TOPIC_NEWS_SENTIMENT in self.topic:
            return self.process_news_message(data)
        elif KafkaConfig.TOPIC_COMPANY_INFO in self.topic:
            return self.process_company_message(data)
        elif KafkaConfig.TOPIC_MARKET_STATUS in self.topic:
            return self.process_market_message(data)
        else:
            logger.warning(f"Unknown topic: {self.topic}")
            return False


def create_ohlc_consumer():
    """Create OHLC data consumer"""
    return StockDataConsumer(
        topic=KafkaConfig.TOPIC_OHLC_DATA,
        group_id='ohlc-processor-group'
    )


def create_news_consumer():
    """Create news sentiment consumer"""
    return StockDataConsumer(
        topic=KafkaConfig.TOPIC_NEWS_SENTIMENT,
        group_id='news-processor-group'
    )


def create_company_consumer():
    """Create company info consumer"""
    return StockDataConsumer(
        topic=KafkaConfig.TOPIC_COMPANY_INFO,
        group_id='company-processor-group'
    )


def create_market_consumer():
    """Create market status consumer"""
    return StockDataConsumer(
        topic=KafkaConfig.TOPIC_MARKET_STATUS,
        group_id='market-processor-group'
    )


if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Allow topic selection from command line
    topic_map = {
        'ohlc': create_ohlc_consumer,
        'news': create_news_consumer,
        'company': create_company_consumer,
        'market': create_market_consumer
    }
    
    if len(sys.argv) > 1:
        topic_name = sys.argv[1].lower()
        if topic_name in topic_map:
            consumer = topic_map[topic_name]()
            consumer.start()
        else:
            print(f"Unknown topic: {topic_name}")
            print(f"Available topics: {', '.join(topic_map.keys())}")
    else:
        print("Usage: python kafka_consumer.py [ohlc|news|company|market]")
        print("\nStarting OHLC consumer by default...")
        consumer = create_ohlc_consumer()
        consumer.start()
