import json
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add paths for imports (works both locally and in Flink pod)
sys.path.insert(0, '/opt/flink/pyjob')  # For pod
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))  # For local

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


class NewsPassThroughFunction(MapFunction):
    """
    Pass-through function - just add processing metadata
    """
    
    def map(self, value: str) -> str:
        try:
            message = json.loads(value)
            
            # Add processing metadata
            message['processed_timestamp'] = datetime.now().isoformat()
            message['processor'] = 'pyflink'
            
            # Extract title for logging
            data = message.get('data', {})
            title = data.get('title', 'No title')
            
            logger.info(f"Processed: {title[:60]}...")
            
            return json.dumps(message)
            
        except Exception as e:
            logger.error(f"Error processing news data: {e}")
            return value


def _configure_jars(env):
    """
    Configure JAR files for PyFlink Kafka connector
    """
    def path_to_file_url(path):
        """Convert Windows/Unix path to file:// URL"""
        abs_path = Path(path).absolute()
        # Convert to POSIX path for URL (forward slashes)
        posix_path = abs_path.as_posix()
        # Add file:// prefix
        if not posix_path.startswith('file://'):
            return f'file:///{posix_path}' if posix_path[1:3] != '://' else f'file://{posix_path}'
        return posix_path
    
    # Check for JAR files in libs directory
    project_root = Path(__file__).parent.parent.parent.parent
    libs_dir = project_root / 'libs'
    
    if libs_dir.exists():
        jar_files = list(libs_dir.glob('*.jar'))
        if jar_files:
            jar_urls = [path_to_file_url(jar) for jar in jar_files]
            logger.info(f"Found {len(jar_files)} JAR file(s) in project libs/")
            for jar in jar_files:
                logger.info(f"   - {jar.name}")
            env.add_jars(*jar_urls)
            return True
    
    # Check Flink lib directory (for K8s deployment)
    flink_lib_dir = Path('/opt/flink/lib')
    if flink_lib_dir.exists():
        jar_files = list(flink_lib_dir.glob('flink-sql-connector-kafka*.jar'))
        if jar_files:
            jar_urls = [path_to_file_url(jar) for jar in jar_files]
            logger.info(f"Found {len(jar_files)} JAR file(s) in Flink lib/")
            for jar in jar_files:
                logger.info(f"   - {jar.name}")
            env.add_jars(*jar_urls)
            return True
    
    # Check environment variable
    env_jars = os.environ.get('PYFLINK_JARS')
    if env_jars:
        jar_list = env_jars.split(';' if os.name == 'nt' else ':')
        jar_urls = [path_to_file_url(jar) for jar in jar_list]
        logger.info(f"Using JAR files from PYFLINK_JARS: {env_jars}")
        env.add_jars(*jar_urls)
        return True
    
    logger.error("")
    logger.error("Kafka connector JAR files not found!")
    logger.error("")
    logger.error("PyFlink requires JAR files to connect to Kafka. Please:")
    logger.error("")
    logger.error("1. Download the JAR file:")
    logger.error("   mkdir libs")
    logger.error("   curl -o libs/flink-sql-connector-kafka-3.1.0-1.18.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar")
    logger.error("")
    logger.error("2. Or set environment variable:")
    logger.error("   Windows: set PYFLINK_JARS=path\\to\\connector.jar")
    logger.error("   Linux/Mac: export PYFLINK_JARS=/path/to/connector.jar")
    logger.error("")
    logger.error("See docs/PYFLINK_SETUP.md for details.")
    logger.error("")
    return False


def create_news_flink_job():
    
    if not PYFLINK_AVAILABLE:
        logger.error("PyFlink is not available")
        return None
    
    # Get execution environment (will run on Flink cluster when submitted via flink run)
    env = StreamExecutionEnvironment.get_execution_environment()
    
    env.set_parallelism(FlinkConfig.DEFAULT_PARALLELISM)
    env.enable_checkpointing(FlinkConfig.CHECKPOINT_INTERVAL_MS)
    
    # Configure JAR files
    if not _configure_jars(env):
        return None
    
    logger.info("="*60)
    logger.info("PyFlink News Sentiment Processing Job")
    logger.info("="*60)
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
    
    processed_stream = news_stream \
        .map(NewsPassThroughFunction(), output_type=Types.STRING()) \
        .name("Pass Through News Data")
    
    # Write to Kafka output topic
    try:
        kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(KafkaConfig.TOPIC_NEWS_PROCESSED)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
            ) \
            .build()
        
        processed_stream.sink_to(kafka_sink).name("Kafka Sink - Processed News")
        logger.info(f"Kafka sink configured to write to: {KafkaConfig.TOPIC_NEWS_PROCESSED}")
    except Exception as e:
        logger.warning(f"Could not configure Kafka sink (will only print): {e}")
        logger.warning(f"    Make sure all required JAR files are available")
    
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
