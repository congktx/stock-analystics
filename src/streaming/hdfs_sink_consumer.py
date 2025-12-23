"""
HDFS Sink Consumer: Kafka to HDFS
Consumes news data from Kafka and writes to HDFS in Parquet format
"""
import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pyarrow as pa
import pyarrow.parquet as pq

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HDFSSinkConsumer:
    """
    Consume news data from Kafka and write to HDFS (or local filesystem) as Parquet
    
    Note: For production HDFS, use hdfs3 library or Kafka Connect HDFS Connector
    This implementation uses local filesystem as fallback
    """
    
    def __init__(
        self,
        hdfs_base_path: str = "/stock-data/news/processed",
        hdfs_namenode = "http://192.168.0.121:9870",
        hdfs_user: str = "root",
        local_fallback: bool = False,
        local_base_path: str = "airflow/data/news/",
        batch_size: int = 100,
        flush_interval_seconds: int = 30
    ):
        """
        Initialize HDFS sink consumer
        
        Args:
            hdfs_base_path: Base path in HDFS
            hdfs_namenode: HDFS NameNode URL (http://localhost:9870 or http://namenode.hadoop.svc.cluster.local:9870)
            hdfs_user: HDFS user for authentication
            local_fallback: Use local filesystem if HDFS not available
            local_base_path: Local path for fallback storage
            batch_size: Number of records before flush
            flush_interval_seconds: Max seconds before flush
        """
        self.hdfs_base_path = hdfs_base_path
        self.hdfs_namenode = hdfs_namenode
        self.hdfs_user = hdfs_user
        self.local_fallback = local_fallback
        self.local_base_path = local_base_path
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        
        # Runtime state
        self.consumer = None
        self.buffer = []
        self.last_flush_time = datetime.now()
        self.messages_consumed = 0
        self.messages_written = 0
        
        # Track files per partition for merging
        self.partition_files = {}  # {partition_path: [file_paths]}
        
        # Try HDFS connection, fallback to local only if allowed
        self.use_local = local_fallback
        self.hdfs_client = None

        if not self.use_local:
            try:
                from hdfs import InsecureClient
                self.hdfs_client = InsecureClient(self.hdfs_namenode, user=self.hdfs_user)
                # Test connection
                self.hdfs_client.status('/')
                logger.info(f" Connected to HDFS NameNode: {self.hdfs_namenode}")
                logger.info(f" HDFS base path: {self.hdfs_base_path}")
                # Create base directory if not exists
                try:
                    if self.hdfs_client is not None:
                        self.hdfs_client.makedirs(self.hdfs_base_path)
                        logger.info(f" Created HDFS directory: {self.hdfs_base_path}")
                except Exception as e:
                    # Directory might already exist
                    pass
            except ImportError:
                logger.error(" hdfs library not installed. Install with: pip install hdfs")
                if not local_fallback:
                    raise Exception("HDFS library required but not installed")
                self.use_local = True
            except Exception as e:
                logger.warning(f" HDFS not available: {e}")
                if not local_fallback:
                    raise
                self.use_local = True

        if self.use_local:
            logger.info(f" Using local storage: {self.local_base_path}")
            Path(self.local_base_path).mkdir(parents=True, exist_ok=True)
    
    def connect_kafka(self):
        """Connect to Kafka"""
        logger.info(f"Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS}")
        
        # Create consumer config with specific group_id for HDFS sink
        consumer_config = KafkaConfig.CONSUMER_CONFIG.copy()
        consumer_config['group_id'] = 'hdfs-sink-group'
        consumer_config['enable_auto_commit'] = False
        
        # Consume from processed topic (after Flink enrichment)
        self.consumer = KafkaConsumer(
            KafkaConfig.TOPIC_NEWS_PROCESSED,
            **consumer_config,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        logger.info(f"Subscribed to topic: {KafkaConfig.TOPIC_NEWS_PROCESSED}")
    
    def get_partition_path(self, timestamp_str: str) -> str:
        """
        Generate Hive-style partitioned path based on timestamp
        
        Args:
            timestamp_str: ISO format timestamp
            
        Returns:
            Path like: ingest_date=2025-12-17
        """
        # Parse time_published to get the date of the news
        try:
            # Try Alpha Vantage format: 20251216T120000
            if 'T' in timestamp_str and len(timestamp_str) == 15:
                dt = datetime.strptime(timestamp_str, '%Y%m%dT%H%M%S')
            else:
                # Try ISO format
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except:
            # Fallback to current date if parsing fails
            dt = datetime.now()
        
        # Use the date from time_published (news publication date)
        ingest_date = dt.strftime('%Y-%m-%d')
        
        return f"ingest_date={ingest_date}"
    
    def flatten_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested message structure for Parquet storage
        
        Args:
            msg: Kafka message (from Flink-processed topic)
            
        Returns:
            Flattened dict
        """
        # Handle both Flink-enriched format and original format
        data = msg.get('data', msg)  # Support both nested and flat structure
        metadata = msg.get('metadata', {})
        sentiment_analytics = msg.get('sentiment_analytics', {})
        
        # Flatten ticker sentiment (take first ticker for simplicity)
        ticker_sentiment = data.get('ticker_sentiment', [])
        primary_ticker = ticker_sentiment[0] if ticker_sentiment else {}
        
        # Add ingest timestamp
        ingest_timestamp = datetime.now().isoformat()
        
        flattened = {
            # Message metadata
            'message_id': msg.get('message_id', ''),
            'source_id': msg.get('source_id', ''),
            'timestamp': msg.get('timestamp', ''),
            'processed_timestamp': msg.get('processed_timestamp', ''),
            'ingest_timestamp': ingest_timestamp,
            'producer': metadata.get('producer', ''),
            'processor': msg.get('processor', ''),
            'partition_key': metadata.get('partition_key', ''),
            
            # News data
            'title': data.get('title', ''),
            'url': data.get('url', ''),
            'time_published': data.get('time_published', ''),
            'summary': data.get('summary', ''),
            'source': data.get('source', ''),
            'source_domain': data.get('source_domain', ''),
            'category_within_source': data.get('category_within_source', ''),
            
            # Sentiment (original)
            'overall_sentiment_score': float(data.get('overall_sentiment_score', 0.0)),
            'overall_sentiment_label': data.get('overall_sentiment_label', ''),
            
            # Sentiment analytics (from Flink)
            'sentiment_category': sentiment_analytics.get('sentiment_category', ''),
            'ticker_count': sentiment_analytics.get('ticker_count', 0),
            'avg_weighted_sentiment': float(sentiment_analytics.get('avg_weighted_sentiment', 0.0)),
            'total_relevance': float(sentiment_analytics.get('total_relevance', 0.0)),
            
            # Primary ticker
            'primary_ticker': primary_ticker.get('ticker', ''),
            'primary_ticker_sentiment_score': float(primary_ticker.get('ticker_sentiment_score', 0.0)),
            'primary_ticker_sentiment_label': primary_ticker.get('ticker_sentiment_label', ''),
            
            # Raw JSON for full data
            'ticker_sentiment_json': json.dumps(ticker_sentiment),
            'sentiment_analytics_json': json.dumps(sentiment_analytics),
            'topics_json': json.dumps(data.get('topics', [])),
            'authors_json': json.dumps(data.get('authors', []))
        }
        
        return flattened
    
    def write_parquet(self, records: List[Dict[str, Any]], partition_path: str):
        """
        Write records to Parquet file
        
        Args:
            records: List of flattened records
            partition_path: Partition path (year=2025/month=12/...)
        """
        if not records:
            return
        
        # Create PyArrow Table
        table = pa.Table.from_pylist(records)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"part-{timestamp}.parquet"
        
        # Full path
        if self.use_local:
            full_path = Path(self.local_base_path) / partition_path
            full_path.mkdir(parents=True, exist_ok=True)
            file_path = full_path / filename
            
            # Write locally
            pq.write_table(table, str(file_path), compression='snappy')
            logger.info(f" Wrote {len(records)} records to {file_path}")
            
            # Track file for potential merging
            if partition_path not in self.partition_files:
                self.partition_files[partition_path] = []
            self.partition_files[partition_path].append(str(file_path))
        else:
            # Write to HDFS
            hdfs_path = f"{self.hdfs_base_path}/{partition_path}/{filename}"
            
            # Ensure partition directory exists
            partition_full_path = f"{self.hdfs_base_path}/{partition_path}"
            try:
                if self.hdfs_client is not None:
                    self.hdfs_client.makedirs(partition_full_path)
            except:
                pass  # Directory might exist
            
            # Write to temp file first
            import tempfile
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.parquet') as temp_file:
                temp_path = temp_file.name
                pq.write_table(table, temp_path, compression='snappy')
            
            # Upload to HDFS
            try:
                if self.hdfs_client is not None:
                    with open(temp_path, 'rb') as f:
                        self.hdfs_client.write(hdfs_path, f, overwrite=False)
                    logger.info(f"Wrote {len(records)} records to HDFS: {hdfs_path}")
                    logger.info(f"   View at: {self.hdfs_namenode}/explorer.html#/{hdfs_path}")
                else:
                    logger.error(" HDFS client is not initialized. Cannot write to HDFS.")
            finally:
                # Clean up temp file
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        
        self.messages_written += len(records)
    
    def merge_partition_files(self, partition_path: str, min_files: int = 5):
        """
        Merge multiple small Parquet files in a partition into one larger file
        
        Args:
            partition_path: Partition path to merge
            min_files: Minimum number of files to trigger merge
        """
        if partition_path not in self.partition_files:
            return
        
        files = self.partition_files[partition_path]
        if len(files) < min_files:
            return
        
        try:
            # Read all files
            tables = []
            for file_path in files:
                if Path(file_path).exists():
                    tables.append(pq.read_table(file_path))
            
            if not tables:
                return
            
            # Concatenate tables
            merged_table = pa.concat_tables(tables)
            
            # Write merged file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            merged_filename = f"merged-{timestamp}.parquet"
            
            if self.use_local:
                full_path = Path(self.local_base_path) / partition_path
                merged_file_path = full_path / merged_filename
                pq.write_table(merged_table, str(merged_file_path), compression='snappy')
                
                # Delete old files
                for file_path in files:
                    if Path(file_path).exists():
                        Path(file_path).unlink()
                
                logger.info(f" Merged {len(files)} files into {merged_file_path} ({len(merged_table)} records)")
                
                # Update tracking
                self.partition_files[partition_path] = [str(merged_file_path)]
        
        except Exception as e:
            logger.error(f" Error merging files in {partition_path}: {e}")
    
    def flush_buffer(self):
        """Flush buffered messages to Parquet files"""
        if not self.buffer:
            return
        
        logger.info(f"Flushing {len(self.buffer)} messages...")
        
        # Group by partition
        partitions = {}
        for msg in self.buffer:
            # Use time_published from news data for partitioning
            data = msg.get('data', {})
            time_published = data.get('time_published', '')
            partition_path = self.get_partition_path(time_published)
            
            if partition_path not in partitions:
                partitions[partition_path] = []
            
            flattened = self.flatten_message(msg)
            partitions[partition_path].append(flattened)
        
        # Write each partition
        for partition_path, records in partitions.items():
            try:
                self.write_parquet(records, partition_path)
                
                # Merge if too many files
                self.merge_partition_files(partition_path, min_files=5)
            except Exception as e:
                logger.error(f"Error writing partition {partition_path}: {e}")
        
        # Clear buffer
        self.buffer.clear()
        self.last_flush_time = datetime.now()
        
        # Commit offset
        if self.consumer:
            self.consumer.commit()
            logger.info(" Kafka offset committed")
    
    def should_flush(self) -> bool:
        """Check if buffer should be flushed"""
        if len(self.buffer) >= self.batch_size:
            return True
        
        elapsed = (datetime.now() - self.last_flush_time).total_seconds()
        if elapsed >= self.flush_interval_seconds and len(self.buffer) > 0:
            return True
        
        return False
    
    def run(self):
        """Main consumer loop"""
        logger.info(" Starting HDFS sink consumer...")
        
        if not self.consumer:
            logger.error(" Kafka consumer not connected. Call connect_kafka() first.")
            return
        
        try:
            for message in self.consumer:
                value = message.value
                self.buffer.append(value)
                self.messages_consumed += 1
                
                # Log progress
                if self.messages_consumed % 100 == 0:
                    logger.info(
                        f"Consumed: {self.messages_consumed}, "
                        f"Written: {self.messages_written}, "
                        f"Buffered: {len(self.buffer)}"
                    )
                
                # Flush if needed
                if self.should_flush():
                    self.flush_buffer()
        
        except KeyboardInterrupt:
            logger.info("\n Interrupted by user")
        finally:
            # Final flush
            if self.buffer:
                logger.info("Final flush...")
                self.flush_buffer()
            
            # Final merge for all partitions
            logger.info("Final merge of partition files...")
            for partition_path in list(self.partition_files.keys()):
                self.merge_partition_files(partition_path, min_files=2)
            
            if self.consumer:
                self.consumer.close()
            
            if self.consumer:
                self.consumer.close()
            
            logger.info(
                f"Consumer stopped. Total consumed: {self.messages_consumed}, "
                f"Total written: {self.messages_written}"
            )


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='HDFS Sink Consumer for News Data')
    parser.add_argument(
        '--hdfs-path',
        default='/stock-data/news/processed',
        help='Base path in HDFS'
    )
    parser.add_argument(
        '--hdfs-namenode',
        default='http://192.168.0.121:9870',
        help='HDFS NameNode URL (e.g., http://namenode.hadoop.svc.cluster.local:9870 for K8s)'
    )
    parser.add_argument(
        '--hdfs-user',
        default='root',
        help='HDFS user for authentication'
    )
    parser.add_argument(
        '--local-path',
        default='airflow/data/news/',
        help='Local fallback path'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size before flush'
    )
    parser.add_argument(
        '--flush-interval',
        type=int,
        default=30,
        help='Flush interval in seconds'
    )
    parser.add_argument(
        '--no-local-fallback',
        action='store_true',
        help='Disable local fallback (HDFS only)'
    )
    parser.add_argument(
        '--use-local',
        action='store_true',
        help='Force local storage (skip HDFS)'
    )
    
    args = parser.parse_args()
    
    # Determine local_fallback based on flags
    if args.use_local:
        local_fallback = True
        logger.info("Forced local storage mode")
    else:
        local_fallback = not args.no_local_fallback
    
    # Create consumer
    consumer = HDFSSinkConsumer(
        hdfs_base_path=args.hdfs_path,
        hdfs_namenode=args.hdfs_namenode,
        hdfs_user=args.hdfs_user,
        local_fallback=not args.no_local_fallback,
        local_base_path=args.local_path,
        batch_size=args.batch_size,
        flush_interval_seconds=args.flush_interval
    )
    
    try:
        consumer.connect_kafka()
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
