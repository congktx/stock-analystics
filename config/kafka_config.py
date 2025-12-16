import os
from dotenv import load_dotenv

load_dotenv()


class KafkaConfig:
    # Kafka Broker Settings
    # K8s NodePort: localhost:30092 (external access)
    # K8s Internal: kafka:9092 (from within pods)
    # Docker: localhost:9092
    BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:30092")
    CLIENT_ID = os.environ.get("KAFKA_CLIENT_ID", "stock-crawler")
    
    # Producer Settings
    PRODUCER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'client_id': CLIENT_ID,
        'acks': 'all',  # Chờ ACK từ tất cả replica
        'retries': 3,
        'max_in_flight_requests_per_connection': 1,  # Đảm bảo thứ tự
        'compression_type': 'gzip',  # Nén dữ liệu gzip
        'batch_size': 16384,  # Batch size 
        'linger_ms': 10,  # 
        'buffer_memory': 33554432,  # 32MB
    }
    
    # Consumer Settings
    CONSUMER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'group_id': 'stock-processor-group',
        'auto_offset_reset': 'earliest',  
        'enable_auto_commit': False,  
        'max_poll_records': 500,
        'session_timeout_ms': 30000,
    }
    
    # Topic Names
    TOPIC_NEWS_SENTIMENT = "stock-news-sentiment"
    TOPIC_OHLC_DATA = "stock-ohlc-data"
    TOPIC_COMPANY_INFO = "stock-company-info"
    TOPIC_MARKET_STATUS = "stock-market-status"
    
    # Dead Letter Queue for failed messages
    TOPIC_DLQ = "stock-data-dlq"
    
    # Topic Configurations
    TOPIC_CONFIGS = {
        TOPIC_NEWS_SENTIMENT: {
            'num_partitions': 8,
            'replication_factor': 1,
            'config': {
                'retention.ms': 604800000,  # 7 days
                'cleanup.policy': 'delete',
                'compression.type': 'gzip',
            }
        },
        TOPIC_OHLC_DATA: {
            'num_partitions': 10,
            'replication_factor': 1,
            'config': {
                'retention.ms': 2592000000,  # 30 days
                'cleanup.policy': 'delete',
                'compression.type': 'gzip',
            }
        },
        TOPIC_COMPANY_INFO: {
            'num_partitions': 4,
            'replication_factor': 1,
            'config': {
                'retention.ms': 31536000000,  # 365 days
                'cleanup.policy': 'compact',  
                'compression.type': 'gzip',
            }
        },
        TOPIC_MARKET_STATUS: {
            'num_partitions': 2,
            'replication_factor': 1,
            'config': {
                'retention.ms': 2592000000,  # 30 days
                'cleanup.policy': 'delete',
                'compression.type': 'gzip',
            }
        },
        TOPIC_DLQ: {
            'num_partitions': 4,
            'replication_factor': 1,
            'config': {
                'retention.ms': 2592000000,  # 30 days
                'cleanup.policy': 'delete',
            }
        }
    }
    
    # Feature Flags
    ENABLE_KAFKA = os.environ.get("ENABLE_KAFKA", "true").lower() == "true"
    FALLBACK_TO_MONGODB = os.environ.get("FALLBACK_TO_MONGODB", "true").lower() == "true"


class FlinkConfig:
    
    # K8s: flink-jobmanager:8081 or localhost:30081
    # Docker: localhost:8081
    JOBMANAGER_URL = os.environ.get("FLINK_JOBMANAGER_URL", "localhost:8081")
    
    # Checkpoint settings
    CHECKPOINT_INTERVAL_MS = 60000  # 1 minute
    CHECKPOINT_MODE = "EXACTLY_ONCE"
    CHECKPOINT_TIMEOUT_MS = 600000  # 10 minutes
    
    # State backend
    STATE_BACKEND = "rocksdb"
    
    # Parallelism
    DEFAULT_PARALLELISM = 4
    MAX_PARALLELISM = 128


if __name__ == "__main__":
    print("Kafka Configuration:")
    print(f"  Bootstrap Servers: {KafkaConfig.BOOTSTRAP_SERVERS}")
    print(f"  Client ID: {KafkaConfig.CLIENT_ID}")
    print(f"  Enable Kafka: {KafkaConfig.ENABLE_KAFKA}")
    print(f"  Fallback to MongoDB: {KafkaConfig.FALLBACK_TO_MONGODB}")
    print(f"\nTopics:")
    for topic_name in [KafkaConfig.TOPIC_NEWS_SENTIMENT, 
                       KafkaConfig.TOPIC_OHLC_DATA,
                       KafkaConfig.TOPIC_COMPANY_INFO, 
                       KafkaConfig.TOPIC_MARKET_STATUS]:
        print(f"  - {topic_name}")
    print(f"\nFlink Configuration:")
    print(f"  JobManager URL: {FlinkConfig.JOBMANAGER_URL}")
    print(f"  Default Parallelism: {FlinkConfig.DEFAULT_PARALLELISM}")
