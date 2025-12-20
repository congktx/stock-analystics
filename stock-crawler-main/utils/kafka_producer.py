"""
Kafka Producer Service for Stock Analytics
"""
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, KafkaTimeoutError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python not installed. Install with: pip install kafka-python")

from kafka_config import KafkaConfig

logger = logging.getLogger(__name__)


class KafkaProducerService:
    """
    Service for producing messages to Kafka topics
    Handles serialization, partitioning, and error handling
    """
    
    def __init__(self, enable_kafka: Optional[bool] = None):
        """
        Initialize Kafka producer
        
        Args:
            enable_kafka: Override config to enable/disable Kafka
        """
        self.enable_kafka = enable_kafka if enable_kafka is not None else KafkaConfig.ENABLE_KAFKA
        self.producer = None
        self.message_count = 0
        self.error_count = 0
        
        if self.enable_kafka and KAFKA_AVAILABLE:
            self._initialize_producer()
        elif self.enable_kafka and not KAFKA_AVAILABLE:
            logger.warning("Kafka enabled but kafka-python not installed")
            self.enable_kafka = False
    
    def _initialize_producer(self):
        """Initialize Kafka producer with configuration"""
        try:
            self.producer = KafkaProducer(
                **KafkaConfig.PRODUCER_CONFIG,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info(f"✓ Kafka producer initialized: {KafkaConfig.BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"✗ Failed to initialize Kafka producer: {e}")
            self.enable_kafka = False
            self.producer = None
    
    def send_message(self, topic: str, value: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Send a message to Kafka topic
        
        Args:
            topic: Kafka topic name
            value: Message payload (will be JSON serialized)
            key: Partition key (optional)
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enable_kafka or not self.producer:
            return False
        
        try:
            # Add metadata
            value['_kafka_timestamp'] = datetime.now().isoformat()
            value['_kafka_source'] = 'stock-crawler'
            
            # Send to Kafka
            future = self.producer.send(topic, value=value, key=key)
            
            # Wait for acknowledgment (with timeout)
            record_metadata = future.get(timeout=10)
            
            self.message_count += 1
            
            logger.debug(
                f"✓ Sent to {topic} "
                f"(partition={record_metadata.partition}, "
                f"offset={record_metadata.offset})"
            )
            
            return True
            
        except KafkaTimeoutError:
            logger.error(f"✗ Timeout sending to {topic}")
            self.error_count += 1
            return False
        except KafkaError as e:
            logger.error(f"✗ Kafka error sending to {topic}: {e}")
            self.error_count += 1
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected error sending to {topic}: {e}")
            self.error_count += 1
            return False
    
    def send_batch(self, topic: str, messages: list, key_field: Optional[str] = None) -> int:
        """
        Send multiple messages to Kafka topic
        
        Args:
            topic: Kafka topic name
            messages: List of message payloads
            key_field: Field name to use as partition key (optional)
        
        Returns:
            int: Number of successfully sent messages
        """
        if not self.enable_kafka or not self.producer:
            return 0
        
        success_count = 0
        
        for msg in messages:
            key = msg.get(key_field) if key_field else None
            if self.send_message(topic, msg, key):
                success_count += 1
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        return success_count
    
    def send_news_sentiment(self, news_data: Dict[str, Any]) -> bool:
        """Send news sentiment data to Kafka"""
        return self.send_message(
            KafkaConfig.TOPIC_NEWS_SENTIMENT,
            news_data,
            key=news_data.get('ticker')
        )
    
    def send_ohlc_data(self, ohlc_data: Dict[str, Any]) -> bool:
        """Send OHLC data to Kafka"""
        return self.send_message(
            KafkaConfig.TOPIC_OHLC_DATA,
            ohlc_data,
            key=ohlc_data.get('ticker')
        )
    
    def send_company_info(self, company_data: Dict[str, Any]) -> bool:
        """Send company info to Kafka"""
        return self.send_message(
            KafkaConfig.TOPIC_COMPANY_INFO,
            company_data,
            key=company_data.get('ticker')
        )
    
    def send_market_status(self, market_data: Dict[str, Any]) -> bool:
        """Send market status to Kafka"""
        return self.send_message(
            KafkaConfig.TOPIC_MARKET_STATUS,
            market_data,
            key=market_data.get('region')
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        return {
            'messages_sent': self.message_count,
            'errors': self.error_count,
            'success_rate': (
                (self.message_count / (self.message_count + self.error_count) * 100)
                if (self.message_count + self.error_count) > 0 else 0
            )
        }
    
    def close(self):
        """Close Kafka producer and clean up resources"""
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info(f"✓ Kafka producer closed. Stats: {self.get_stats()}")
            except Exception as e:
                logger.error(f"✗ Error closing Kafka producer: {e}")


# Global producer instance (singleton pattern)
_producer_instance = None


def get_producer() -> KafkaProducerService:
    """Get or create global Kafka producer instance"""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = KafkaProducerService()
    return _producer_instance


if __name__ == "__main__":
    # Test producer
    logging.basicConfig(level=logging.INFO)
    
    producer = KafkaProducerService()
    
    if producer.enable_kafka:
        # Test message
        test_data = {
            "ticker": "TEST",
            "price": 100.50,
            "timestamp": datetime.now().isoformat()
        }
        
        success = producer.send_message(KafkaConfig.TOPIC_OHLC_DATA, test_data, key="TEST")
        print(f"Test message sent: {success}")
        print(f"Stats: {producer.get_stats()}")
        
        producer.close()
    else:
        print("Kafka is disabled or not available")
