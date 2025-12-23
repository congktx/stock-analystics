import logging
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

sys.path.insert(0, '.')
from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_news_topics():
    
    logger.info(f"Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS}")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            client_id='topic-creator'
        )
        
        # Topics for news pipeline
        topics_to_create = [
            KafkaConfig.TOPIC_NEWS_SENTIMENT,
            KafkaConfig.TOPIC_NEWS_PROCESSED,
            KafkaConfig.TOPIC_SQL_MAPPING,
            KafkaConfig.TOPIC_SENTIMENT_DAILY,
            KafkaConfig.TOPIC_RAW_SQL_DATA
        ]
        
        topic_configs = KafkaConfig.TOPIC_CONFIGS
        
        new_topics = []
        for topic_name in topics_to_create:
            config = topic_configs.get(topic_name, {})
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=config.get('num_partitions', 8),
                replication_factor=config.get('replication_factor', 1),
                topic_configs=config.get('config', {})
            )
            new_topics.append(topic)
        
        logger.info(f"Creating {len(new_topics)} topics...")
        
        for topic in new_topics:
            logger.info(f"  - {topic.name} (partitions={topic.num_partitions})")
        
        # Create topics - this may raise TopicAlreadyExistsError
        try:
            result = admin_client.create_topics(new_topics=new_topics, validate_only=False)
            
            # Check results - result is a dict-like object
            for topic_name in topics_to_create:
                try:
                    if hasattr(result, 'topic_errors'):
                        # Check topic_errors attribute
                        errors = result.topic_errors
                        if topic_name in errors:
                            error = errors[topic_name]
                            if error.error_code == 0:
                                logger.info(f"Created topic: {topic_name}")
                            elif error.error_code == 36:  # TopicAlreadyExists
                                logger.warning(f"Topic already exists: {topic_name}")
                            else:
                                logger.error(f"Failed to create topic {topic_name}: {error}")
                        else:
                            logger.info(f"Created topic: {topic_name}")
                    else:
                        # Fallback: assume success if no errors
                        logger.info(f"Created topic: {topic_name}")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
        
        except TopicAlreadyExistsError as e:
            # Parse the error message to see which topics already exist
            logger.warning("Some topics already exist:")
            for topic_name in topics_to_create:
                if topic_name in str(e):
                    logger.warning(f"   - {topic_name}")
        
        except Exception as e:
            # For other errors, let it propagate
            raise e
        
        admin_client.close()
        
        logger.info("")
        logger.info("="*60)
        logger.info("Kafka topics setup complete!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
        logger.error("")
        logger.error("Make sure Kafka is running and accessible at:")
        logger.error(f"  {KafkaConfig.BOOTSTRAP_SERVERS}")
        logger.error("")
        logger.error("For K8s deployment, ensure port-forward is active:")
        logger.error("  kubectl port-forward svc/kafka 30092:9092")
        logger.error("")
        sys.exit(1)


def list_topics():
    """List all existing Kafka topics"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            client_id='topic-lister'
        )
        
        topics = admin_client.list_topics()
        
        logger.info("")
        logger.info("Existing Kafka Topics:")
        logger.info("-" * 60)
        for topic in sorted(topics):
            # Check if it's a news pipeline topic
            if 'news' in topic:
                logger.info(f"  {topic}")
            else:
                logger.info(f"  {topic}")
        logger.info("-" * 60)
        logger.info(f"Total: {len(topics)} topics")
        logger.info("")
        
        admin_client.close()
        
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        sys.exit(1)


def delete_news_topics():
    """Delete news pipeline topics (for cleanup)"""
    logger.warning("This will delete news pipeline topics!")
    response = input("Are you sure? (yes/no): ")
    
    if response.lower() != 'yes':
        logger.info("Cancelled")
        return
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            client_id='topic-deleter'
        )
        
        topics_to_delete = [
            KafkaConfig.TOPIC_NEWS_SENTIMENT,
            KafkaConfig.TOPIC_NEWS_PROCESSED
        ]
        
        logger.info(f"Deleting {len(topics_to_delete)} topics...")
        
        result = admin_client.delete_topics(topics=topics_to_delete)
        
        for topic_name in topics_to_delete:
            try:
                if hasattr(result, 'topic_error_codes'):
                    errors = result.topic_error_codes
                    if topic_name in errors:
                        error_code = errors[topic_name]
                        if error_code == 0:
                            logger.info(f"Deleted topic: {topic_name}")
                        else:
                            logger.error(f"Failed to delete topic {topic_name}: error code {error_code}")
                    else:
                        logger.info(f"Deleted topic: {topic_name}")
                else:
                    logger.info(f"Deleted topic: {topic_name}")
            except Exception as e:
                logger.error(f"Failed to delete topic {topic_name}: {e}")
        
        admin_client.close()
        logger.info("Topics deleted")
        
    except Exception as e:
        logger.error(f"Error deleting topics: {e}")
        sys.exit(1)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Setup Kafka Topics for News Pipeline')
    parser.add_argument('action', choices=['create', 'list', 'delete'], help='Action to perform')
    
    args = parser.parse_args()
    
    if args.action == 'create':
        create_news_topics()
    elif args.action == 'list':
        list_topics()
    elif args.action == 'delete':
        delete_news_topics()


if __name__ == '__main__':
    main()
