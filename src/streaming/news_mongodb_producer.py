import os
import sys
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import logging

from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config.kafka_config import KafkaConfig
from src.storage.redis_cache import RedisCache

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NewsMongoDBProducer:
    """Produce news data from MongoDB to Kafka"""
    
    def __init__(
        self,
        mongodb_uri: str = "mongodb://localhost:27017",
        mongodb_database: str = "stock_analytics",
        mongodb_collection: str = "news_sentiment",
        batch_size: int = 100,
        enable_deduplication: bool = True
    ):
        """
        Initialize MongoDB to Kafka producer
        
        Args:
            mongodb_uri: MongoDB connection string
            mongodb_database: Database name
            mongodb_collection: Collection name
            batch_size: Number of records to process per batch
            enable_deduplication: Use Redis to track processed messages
        """
        self.mongodb_uri = mongodb_uri
        self.mongodb_database = mongodb_database
        self.mongodb_collection = mongodb_collection
        self.batch_size = batch_size
        self.enable_deduplication = enable_deduplication
        
        # Initialize connections
        self.mongo_client = None
        self.kafka_producer = None
        self.redis_cache = None
        
        # Metrics
        self.messages_sent = 0
        self.messages_failed = 0
        self.messages_skipped = 0
        
    def connect(self):
        """Establish connections to MongoDB, Kafka, and Redis"""
        try:
            # MongoDB
            logger.info(f"Connecting to MongoDB: {self.mongodb_uri}")
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.db = self.mongo_client[self.mongodb_database]
            self.collection = self.db[self.mongodb_collection]
            
            # Test connection
            self.mongo_client.server_info()
            logger.info(f"Connected to MongoDB database: {self.mongodb_database}")
            
            # Kafka
            logger.info(f"Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS}")
            self.kafka_producer = KafkaProducer(
                **KafkaConfig.PRODUCER_CONFIG,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info(f"Kafka producer initialized")
            
            # Redis (optional)
            if self.enable_deduplication:
                try:
                    self.redis_cache = RedisCache()
                    logger.info("Redis cache connected for deduplication")
                except Exception as e:
                    logger.warning(f"Redis not available, deduplication disabled: {e}")
                    self.enable_deduplication = False
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
    
    def close(self):
        """Close all connections"""
        if self.kafka_producer:
            logger.info("Flushing Kafka producer...")
            self.kafka_producer.flush()
            self.kafka_producer.close()
        
        if self.mongo_client:
            self.mongo_client.close()
        
        if self.redis_cache:
            self.redis_cache.close()
        
        logger.info(f"Connections closed. Stats: Sent={self.messages_sent}, "
                   f"Failed={self.messages_failed}, Skipped={self.messages_skipped}")
    
    def is_already_processed(self, doc_id: str) -> bool:
        """Check if document has already been processed using Redis"""
        if not self.enable_deduplication or not self.redis_cache:
            return False
        
        cache_key = f"news_processed:{doc_id}"
        return self.redis_cache.get(cache_key) is not None
    
    def mark_as_processed(self, doc_id: str, ttl_days: int = 30):
        """Mark document as processed in Redis"""
        if self.enable_deduplication and self.redis_cache:
            cache_key = f"news_processed:{doc_id}"
            self.redis_cache.set(cache_key, "1", ttl=ttl_days * 86400)
    
    def transform_document(self, doc: Dict[str, Any]) -> tuple[Dict[str, Any], str]:
        """
        Transform MongoDB document to Kafka message format
        
        Args:
            doc: MongoDB document
            
        Returns:
            Tuple of (message dict, partition key)
        """
        doc_id = str(doc.get('_id', ''))
        
        # Parse ticker_sentiment if it's a string
        ticker_sentiment_raw = doc.get('ticker_sentiment', [])
        if isinstance(ticker_sentiment_raw, str):
            try:
                # Replace single quotes with double quotes for JSON parsing
                ticker_sentiment_json = ticker_sentiment_raw.replace("'", '"')
                ticker_sentiment = json.loads(ticker_sentiment_json)
                if ticker_sentiment:
                    logger.debug(f"Parsed {len(ticker_sentiment)} tickers for {doc_id}")
            except Exception as e:
                logger.warning(f"Failed to parse ticker_sentiment for {doc_id}: {e}")
                ticker_sentiment = []
        else:
            ticker_sentiment = ticker_sentiment_raw if isinstance(ticker_sentiment_raw, list) else []
        
        # Parse authors if it's a string
        authors_raw = doc.get('authors', [])
        if isinstance(authors_raw, str):
            try:
                authors_json = authors_raw.replace("'", '"')
                authors = json.loads(authors_json)
            except:
                authors = [authors_raw] if authors_raw else []
        else:
            authors = authors_raw if isinstance(authors_raw, list) else []
        
        # Parse topics if it's a string
        topics_raw = doc.get('topics', [])
        if isinstance(topics_raw, str):
            try:
                topics_json = topics_raw.replace("'", '"')
                topics = json.loads(topics_json)
            except:
                topics = []
        else:
            topics = topics_raw if isinstance(topics_raw, list) else []
        
        # Parse sentiment score (handle string literals)
        sentiment_score_raw = doc.get('overall_sentiment_score', 0.0)
        try:
            sentiment_score = float(sentiment_score_raw) if sentiment_score_raw and sentiment_score_raw != 'overall_sentiment_score' else 0.0
        except (ValueError, TypeError):
            sentiment_score = 0.0
        
        # Extract main ticker for partition key
        partition_key = ticker_sentiment[0]['ticker'] if ticker_sentiment and len(ticker_sentiment) > 0 else 'UNKNOWN'
        
        message = {
            'message_id': str(uuid.uuid4()),
            'source_id': doc_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': {
                'title': doc.get('title', ''),
                'url': doc.get('url', ''),
                'time_published': doc.get('time_published', ''),
                'authors': authors,
                'summary': doc.get('summary', ''),
                'source': doc.get('source', ''),
                'category_within_source': doc.get('category_within_source', ''),
                'source_domain': doc.get('source_domain', ''),
                'topics': topics,
                'overall_sentiment_score': sentiment_score,
                'overall_sentiment_label': doc.get('overall_sentiment_label', 'Neutral'),
                'ticker_sentiment': ticker_sentiment
            },
            'metadata': {
                'producer': 'mongodb-news-producer',
                'version': '1.0',
                'partition_key': partition_key,
                'mongodb_id': doc_id
            }
        }
        
        return message, partition_key
    
    def send_message(self, message: Dict[str, Any], partition_key: str) -> bool:
        """
        Send message to Kafka topic
        
        Args:
            message: Message data
            partition_key: Partition key (ticker symbol)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.kafka_producer:
            logger.error("Kafka producer not connected")
            return False
        
        try:
            future = self.kafka_producer.send(
                KafkaConfig.TOPIC_NEWS_SENTIMENT,
                value=message,
                key=partition_key
            )
            
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Sent message {message['message_id']} "
                f"to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )
            
            self.messages_sent += 1
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error sending message: {e}")
            self.messages_failed += 1
            return False
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.messages_failed += 1
            return False
    
    def process_batch(
        self,
        query: Optional[Dict] = None,
        limit: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> int:
        """
        Process a batch of news documents from MongoDB
        
        Args:
            query: MongoDB query filter (default: all documents)
            limit: Maximum number of documents to process
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
            
        Returns:
            Number of messages sent
        """
        if query is None:
            # Skip header document (where title is literal "title")
            query = {'title': {'$ne': 'title'}}
        
        # Add date range filter
        if start_date or end_date:
            from datetime import datetime
            if 'time_published' not in query:
                query['time_published'] = {}
            
            if start_date:
                # Convert YYYY-MM-DD to Alpha Vantage format: YYYYMMDD
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                query['time_published']['$gte'] = start_dt.strftime('%Y%m%dT%H%M%S')
                logger.info(f"Filtering news from: {start_date}")
            
            if end_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                # Add one day and use < instead of <=
                from datetime import timedelta
                end_dt = end_dt + timedelta(days=1)
                query['time_published']['$lt'] = end_dt.strftime('%Y%m%dT%H%M%S')
                logger.info(f"Filtering news until: {end_date}")
        
        if not self.kafka_producer:
            logger.error("Kafka producer not connected. Call connect() first.")
            return 0
        
        # Find documents
        cursor = self.collection.find(query).batch_size(self.batch_size)
        if limit:
            cursor = cursor.limit(limit)
        
        batch_sent = 0
        batch_start = time.time()
        
        for doc in cursor:
            doc_id = str(doc.get('_id', ''))
            
            # Skip if already processed
            if self.is_already_processed(doc_id):
                self.messages_skipped += 1
                continue
            
            # Transform and send
            try:
                message, partition_key = self.transform_document(doc)
                
                if self.send_message(message, partition_key):
                    self.mark_as_processed(doc_id)
                    batch_sent += 1
                    
                    # Log progress every 100 messages
                    if batch_sent % 100 == 0:
                        elapsed = time.time() - batch_start
                        rate = batch_sent / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Progress: {batch_sent} messages sent "
                            f"({rate:.1f} msg/sec)"
                        )
            
            except Exception as e:
                logger.error(f"Error processing document {doc_id}: {e}")
                self.messages_failed += 1
                continue
        
        # Flush remaining messages
        if self.kafka_producer:
            self.kafka_producer.flush()
        
        elapsed = time.time() - batch_start
        rate = batch_sent / elapsed if elapsed > 0 else 0
        logger.info(
            f"Batch complete: {batch_sent} messages sent in {elapsed:.1f}s "
            f"({rate:.1f} msg/sec)"
        )
        
        return batch_sent
    
    def run_continuous(self, poll_interval_seconds: int = 60):
        """
        Continuously poll MongoDB for new data
        
        Args:
            poll_interval_seconds: Seconds to wait between polls
        """
        logger.info(f"Starting continuous mode (poll interval: {poll_interval_seconds}s)")
        
        try:
            while True:
                logger.info("Polling for new documents...")
                count = self.process_batch()
                
                if count == 0:
                    logger.info("No new documents found")
                
                logger.info(f"Waiting {poll_interval_seconds}s before next poll...")
                time.sleep(poll_interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("\nInterrupted by user")
        except Exception as e:
            logger.error(f"Error in continuous mode: {e}")
            raise
        finally:
            self.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MongoDB to Kafka News Producer')
    parser.add_argument(
        '--mongodb-uri',
        default='mongodb://localhost:27017',
        help='MongoDB connection URI'
    )
    parser.add_argument(
        '--database',
        default='stock_analytics',
        help='MongoDB database name'
    )
    parser.add_argument(
        '--collection',
        default='news_sentiment',
        help='MongoDB collection name'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for processing'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of documents (default: all)'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run in continuous mode'
    )
    parser.add_argument(
        '--poll-interval',
        type=int,
        default=60,
        help='Poll interval in seconds for continuous mode'
    )
    parser.add_argument(
        '--no-dedup',
        action='store_true',
        help='Disable deduplication'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date filter (YYYY-MM-DD format, filters time_published >= start_date)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date filter (YYYY-MM-DD format, filters time_published <= end_date)'
    )
    
    args = parser.parse_args()
    
    # Create producer
    producer = NewsMongoDBProducer(
        mongodb_uri=args.mongodb_uri,
        mongodb_database=args.database,
        mongodb_collection=args.collection,
        batch_size=args.batch_size,
        enable_deduplication=not args.no_dedup
    )
    
    try:
        # Connect
        producer.connect()
        
        # Run
        if args.continuous:
            producer.run_continuous(poll_interval_seconds=args.poll_interval)
        else:
            producer.process_batch(
                limit=args.limit,
                start_date=args.start_date,
                end_date=args.end_date
            )
            producer.close()
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
