"""
Simple Kafka Consumer for Processing Stock Data
Thay thế cho Flink jobs - đơn giản hơn để test
"""
import json
import logging
import signal
import sys
from kafka import KafkaConsumer
from datetime import datetime
from kafka_config import KafkaConfig
from database.mongodb import MongoDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockDataProcessor:
    """Process stock data from Kafka and save to MongoDB"""
    
    def __init__(self):
        self.mongodb = MongoDB()
        self.running = True
        self.processed_count = 0
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Graceful shutdown"""
        logger.info("\nShutting down processor...")
        self.running = False
    
    def process_ohlc_message(self, message):
        """Process OHLC data and add technical indicators"""
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            # Add processing metadata
            data['processed_at'] = datetime.utcnow().isoformat()
            data['processor'] = 'kafka_consumer'
            
            # Calculate simple technical indicators
            close_price = data.get('c', 0)
            high_price = data.get('h', 0)
            low_price = data.get('l', 0)
            
            # Price range
            price_range = high_price - low_price
            
            # Add indicators
            data['technical_indicators'] = {
                'price_range': round(price_range, 2),
                'price_range_pct': round((price_range / close_price * 100), 2) if close_price > 0 else 0,
                'volume': data.get('v', 0),
                'vwap': data.get('vw', 0)
            }
            
            # Save to MongoDB
            collection = self.mongodb.get_collection('OHLC_processed')
            result = collection.update_one(
                {'_id': data['_id']},
                {'$set': data},
                upsert=True
            )
            
            logger.info(
                f"✓ Processed OHLC: {data.get('ticker')} - "
                f"Close: ${close_price} - "
                f"Range: {price_range:.2f} ({data['technical_indicators']['price_range_pct']:.2f}%)"
            )
            
            self.processed_count += 1
            return True
            
        except Exception as e:
            logger.error(f"Error processing OHLC: {e}")
            return False
    
    def process_news_message(self, message):
        """Process news sentiment data"""
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            # Add processing metadata
            data['processed_at'] = datetime.utcnow().isoformat()
            data['processor'] = 'kafka_consumer'
            
            # Extract sentiment metrics
            ticker_sentiments = data.get('ticker_sentiment', [])
            
            sentiment_summary = {
                'ticker_count': len(ticker_sentiments),
                'overall_sentiment': data.get('overall_sentiment_label', 'Unknown'),
                'overall_score': data.get('overall_sentiment_score', 0),
                'tickers': []
            }
            
            for ticker_data in ticker_sentiments:
                ticker = ticker_data.get('ticker')
                sentiment_summary['tickers'].append({
                    'ticker': ticker,
                    'sentiment': ticker_data.get('ticker_sentiment_label'),
                    'score': ticker_data.get('ticker_sentiment_score', 0),
                    'relevance': ticker_data.get('relevance_score', 0)
                })
            
            data['sentiment_summary'] = sentiment_summary
            
            # Save to MongoDB
            collection = self.mongodb.get_collection('news_sentiment_processed')
            result = collection.update_one(
                {'_id': data['_id']},
                {'$set': data},
                upsert=True
            )
            
            logger.info(
                f"✓ Processed News: {data.get('title', 'No title')[:50]}... - "
                f"Sentiment: {sentiment_summary['overall_sentiment']} - "
                f"Tickers: {len(ticker_sentiments)}"
            )
            
            self.processed_count += 1
            return True
            
        except Exception as e:
            logger.error(f"Error processing news: {e}")
            return False
    
    def start_ohlc_consumer(self):
        """Start consuming OHLC data from Kafka"""
        logger.info("Starting OHLC data consumer...")
        logger.info(f"Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS}")
        logger.info(f"Topic: {KafkaConfig.TOPIC_OHLC_DATA}")
        
        consumer = KafkaConsumer(
            KafkaConfig.TOPIC_OHLC_DATA,
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            group_id='ohlc-processor-group',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: m
        )
        
        logger.info("✓ Consumer connected. Waiting for messages...")
        
        try:
            for message in consumer:
                if not self.running:
                    break
                
                success = self.process_ohlc_message(message)
                
                if success:
                    # Commit offset after successful processing
                    consumer.commit()
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info(f"OHLC Consumer stopped. Processed {self.processed_count} messages.")
    
    def start_news_consumer(self):
        """Start consuming news sentiment data from Kafka"""
        logger.info("Starting News sentiment consumer...")
        logger.info(f"Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS}")
        logger.info(f"Topic: {KafkaConfig.TOPIC_NEWS_SENTIMENT}")
        
        consumer = KafkaConsumer(
            KafkaConfig.TOPIC_NEWS_SENTIMENT,
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            group_id='news-processor-group',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: m
        )
        
        logger.info("✓ Consumer connected. Waiting for messages...")
        
        try:
            for message in consumer:
                if not self.running:
                    break
                
                success = self.process_news_message(message)
                
                if success:
                    consumer.commit()
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info(f"News Consumer stopped. Processed {self.processed_count} messages.")


def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("Stock Data Processor (Kafka Consumer)")
    logger.info("=" * 60)
    
    print("\nSelect processor to run:")
    print("1. OHLC Data Processor")
    print("2. News Sentiment Processor")
    print("3. Exit")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    processor = StockDataProcessor()
    
    if choice == '1':
        logger.info("\nStarting OHLC processor...")
        logger.info("Press Ctrl+C to stop")
        processor.start_ohlc_consumer()
    elif choice == '2':
        logger.info("\nStarting News processor...")
        logger.info("Press Ctrl+C to stop")
        processor.start_news_consumer()
    elif choice == '3':
        logger.info("Exiting...")
    else:
        logger.error("Invalid choice")


if __name__ == "__main__":
    main()
