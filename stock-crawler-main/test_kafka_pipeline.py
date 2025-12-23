import time
import logging
import subprocess
from kafka import KafkaConsumer, KafkaProducer
from kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineHealthCheck:
    @staticmethod
    def check_kafka():
        """Check Kafka"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                request_timeout_ms=5000
            )
            producer.close()
            logger.info("Kafka is accessible at %s", KafkaConfig.BOOTSTRAP_SERVERS)
            return True
        except Exception as e:
            logger.error("Kafka is not accessible: %s", e)
            return False
    
    @staticmethod
    def check_mongodb():
        """Check MongoDB"""
        try:
            from database.mongodb import MongoDB
            db = MongoDB()
            # Try to fetch one document
            collection = db.get_collection('OHLC')
            collection.find_one()
            logger.info("MongoDB is accessible")
            return True
        except Exception as e:
            logger.error("MongoDB is not accessible: %s", e)
            return False
    
    @staticmethod
    def check_flink():
        """Check Flink (K8s)"""
        try:
            result = subprocess.run(
                ['kubectl', 'get', 'pods', '-n', 'stock-analytics', '-l', 'app=flink'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if 'Running' in result.stdout:
                logger.info("Flink pods are running")
                return True
            else:
                logger.warning("Flink pods status unclear")
                return False
        except Exception as e:
            logger.error("Cannot check Flink status: %s", e)
            return False
    
    @staticmethod
    def list_kafka_topics():
        """List Kafka topics"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                request_timeout_ms=5000
            )
            topics = consumer.topics()
            consumer.close()
            
            logger.info("Available Kafka topics:")
            for topic in sorted(topics):
                logger.info("  - %s", topic)
            return list(topics)
        except Exception as e:
            logger.error("Failed to list topics: %s", e)
            return []


class KafkaTopicMonitor:
    """Monitor Kafka topics"""
    
    def __init__(self, topic, group_id=None):
        self.topic = topic
        self.group_id = group_id or f"monitor-{topic}"
        
    def consume_recent_messages(self, max_messages=5, timeout_ms=10000):
        """Consume and display recent messages from topic"""
        logger.info(f"Monitoring topic: {self.topic}")
        
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            auto_offset_reset='latest', \
            value_deserializer=lambda m: m.decode('utf-8'),
            consumer_timeout_ms=timeout_ms
        )
        
        messages_received = 0
        try:
            for message in consumer:
                logger.info(
                    f"Message {messages_received + 1}:\n"
                    f"  Topic: {message.topic}\n"
                    f"  Partition: {message.partition}\n"
                    f"  Offset: {message.offset}\n"
                    f"  Key: {message.key}\n"
                    f"  Value (first 200 chars): {message.value[:200]}..."
                )
                messages_received += 1
                
                if messages_received >= max_messages:
                    break
                    
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            consumer.close()
        
        logger.info(f"Total messages consumed: {messages_received}")
        return messages_received


def run_health_checks():
    logger.info("Pipeline Health Checks")
    
    health = PipelineHealthCheck()
    
    kafka_ok = health.check_kafka()
    mongodb_ok = health.check_mongodb()
    flink_ok = health.check_flink()
    
    logger.info("\n" + "=" * 60)
    if kafka_ok and mongodb_ok:
        logger.info("Core services are healthy")
        return True
    else:
        logger.error("Some services are not healthy")
        return False


def stream_test_data():
    """Stream test data from MongoDB to Kafka"""
    
    try:
        from test_real_streaming import RealDataStreamer
        
        streamer = RealDataStreamer()
        
        # Stream OHLC 
        logger.info("\nStreaming OHLC...")
        tickers = streamer.list_available_tickers(limit=5)
        if tickers:
            streamer.stream_ohlc_to_kafka(ticker=tickers[0], batch_size=3, interval=1)
        
        time.sleep(2)
        
        # Stream News data
        logger.info("\nStreaming News data...")
        streamer.stream_news_to_kafka(batch_size=3, interval=1)
        
        streamer.close()
        logger.info("Data streaming completed")
        return True
        
    except Exception as e:
        logger.error(f"Failed to stream data: {e}", exc_info=True)
        return False


def monitor_kafka_topics():
    """Monitor Kafka topics"""
    
    # List topics
    health = PipelineHealthCheck()
    topics = health.list_kafka_topics()
    
    # Monitor OHLC topic
    if KafkaConfig.TOPIC_OHLC_DATA in topics:
        logger.info(f"\nMonitoring {KafkaConfig.TOPIC_OHLC_DATA}...")
        monitor = KafkaTopicMonitor(KafkaConfig.TOPIC_OHLC_DATA)
        monitor.consume_recent_messages(max_messages=2, timeout_ms=5000)
    
    # Monitor News topic
    if KafkaConfig.TOPIC_NEWS_SENTIMENT in topics:
        logger.info(f"\nMonitoring {KafkaConfig.TOPIC_NEWS_SENTIMENT}...")
        monitor = KafkaTopicMonitor(KafkaConfig.TOPIC_NEWS_SENTIMENT)
        monitor.consume_recent_messages(max_messages=2, timeout_ms=5000)


def main():
    # Health checks
    if not run_health_checks():
        logger.error("\nHealth checks failed. Please fix the issues and try again.")
        logger.info("\nTroubleshooting:")
        logger.info("- Kafka: kubectl get pods -n stock-analytics")
        logger.info("- MongoDB: Check connection string in .env")
        logger.info("- Flink: kubectl logs -n stock-analytics -l app=flink")
        return
    
    time.sleep(2)
    
    # Stream real data
    if not stream_test_data():
        logger.error("\nData streaming failed.")
        return
    
    time.sleep(3)
    
    # Monitor topics
    monitor_kafka_topics()

if __name__ == "__main__":
    main()
