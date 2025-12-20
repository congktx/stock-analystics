import logging
from typing import Dict, List

try:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("kafka-python not installed")

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'stock-crawler-main'))

from kafka_config import KafkaConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_topics():
    """Create Kafka topics"""
    
    if not KAFKA_AVAILABLE:
        logger.error("kafka-python not installed. Install with: pip install kafka-python")
        return False
    
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            client_id='topic-creator'
        )
        
        # Prepare topic list
        topics_to_create = []
        
        for topic_name, config in KafkaConfig.TOPIC_CONFIGS.items():
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=config['num_partitions'],
                replication_factor=config['replication_factor'],
                topic_configs=config.get('config', {})
            )
            topics_to_create.append(new_topic)
        
        # Create topics
        logger.info(f"Creating {len(topics_to_create)} Kafka topics...")
        
        result = admin_client.create_topics(
            new_topics=topics_to_create,
            validate_only=False
        )
        
        # Check results - result is a dict of topic_name -> future
        created_count = 0
        for topic_name, future in result.topic_errors():
            try:
                if future is None or future == 0:  # Success
                    logger.info(f"Created topic: {topic_name}")
                    created_count += 1
                elif future == 36:  # TopicAlreadyExistsError 
                    logger.info(f"Topic already exists: {topic_name}")
                else:
                    logger.error(f"Error creating topic {topic_name}: error code {future}")
            except Exception as e:
                logger.error(f"Error checking topic {topic_name}: {e}")
        
        admin_client.close()
        logger.info("Topic creation completed")
        return True
        
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


def list_topics():
    """List Kafka topics"""
    
    if not KAFKA_AVAILABLE:
        logger.error("kafka-python not installed")
        return []
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            client_id='topic-lister'
        )
        
        topics = admin_client.list_topics()
        logger.info(f"Found {len(topics)} topics:")
        for topic in sorted(topics):
            logger.info(f"  - {topic}")
        
        admin_client.close()
        return topics
        
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        return []


def delete_topics(topic_names: List[str]):
    """Delete specified Kafka topics"""
    
    if not KAFKA_AVAILABLE:
        logger.error("kafka-python not installed")
        return False
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            client_id='topic-deleter'
        )
        
        logger.info(f"Deleting {len(topic_names)} topics...")
        result = admin_client.delete_topics(topic_names)
        
        for topic_name, future in result.items():
            try:
                future.result()
                logger.info(f"Deleted topic: {topic_name}")
            except Exception as e:
                logger.error(f"Error deleting topic {topic_name}: {e}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return False


def describe_topic(topic_name: str):
    
    if not KAFKA_AVAILABLE:
        logger.error("kafka-python not installed")
        return None
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            client_id='topic-describer'
        )
        
        metadata = admin_client.describe_topics([topic_name])
        
        for topic in metadata:
            logger.info(f"\nTopic: {topic['topic']}")
            logger.info(f"  Partitions: {len(topic['partitions'])}")
            for partition in topic['partitions']:
                logger.info(f"    Partition {partition['partition']}: Leader={partition['leader']}")
        
        admin_client.close()
        return metadata
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return None


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python setup_kafka_topics.py create    - Create all topics")
        print("  python setup_kafka_topics.py list      - List all topics")
        print("  python setup_kafka_topics.py describe <topic>  - Describe a topic")
        print("  python setup_kafka_topics.py delete <topic1> <topic2>  - Delete topics")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'create':
        create_topics()
    elif command == 'list':
        list_topics()
    elif command == 'describe' and len(sys.argv) >= 3:
        describe_topic(sys.argv[2])
    elif command == 'delete' and len(sys.argv) >= 3:
        delete_topics(sys.argv[2:])
    else:
        print("Invalid command or missing arguments")
        sys.exit(1)
