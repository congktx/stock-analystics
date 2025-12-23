import os
import sys
import json
import logging
import datetime as dt
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pyodbc

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SQLServerSink:
    """SQL Server connection and operations"""
    
    def __init__(
        self,
        server: str = "192.168.0.121",
        port: int = 1433,
        database: str = "Streaming",
        username: Optional[str] = "sa",
        password: Optional[str] = "YourStrong@Password123",
        driver: str = "{ODBC Driver 17 for SQL Server}"
    ):
        """Initialize SQL Server connection"""
        self.server = server
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish SQL Server connection"""
        # Use Windows Authentication if username/password not provided
        if self.username and self.password:
            conn_str = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password}"
            )
        else:
            conn_str = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes"
            )
        
        try:
            self.conn = pyodbc.connect(conn_str, autocommit=False)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQL Server: {self.server}/{self.database}")
        except Exception as e:
            logger.error(f"SQL Server connection error: {e}")
            raise
    
    def close(self):
        """Close SQL Server connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("SQL Server connection closed")
    
    def insert_news_sentiment(self, records: List[Dict[str, Any]]) -> int:
        """
        Batch insert news sentiment records
        
        Args:
            records: List of news records
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
        
        if not self.cursor or not self.conn:
            logger.error("Database connection not established")
            return 0
        
        sql = """
        INSERT INTO news_sentiment_processed (
            message_id, source_id, title, url, time_published,
            summary, source_name, overall_sentiment_score, overall_sentiment_label
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        values = []
        for record in records:
            data = record.get('data', {})
            
            # Parse time_published
            time_published = self._parse_time_published(data.get('time_published', ''))
            
            values.append((
                record.get('message_id', ''),
                record.get('source_id', ''),
                data.get('title', ''),
                data.get('url', ''),
                time_published,
                data.get('summary', ''),
                data.get('source', ''),
                float(data.get('overall_sentiment_score', 0.0)),
                data.get('overall_sentiment_label', '')
            ))
        
        try:
            self.cursor.executemany(sql, values)
            self.conn.commit()
            logger.info(f"Inserted {len(records)} news records")
            return len(records)
        except pyodbc.IntegrityError as e:
            # Handle duplicates
            if "duplicate key" in str(e).lower() or "unique" in str(e).lower():
                logger.warning(f"Duplicate records skipped: {e}")
                self.conn.rollback()
                return 0
            else:
                logger.error(f"Insert error: {e}")
                self.conn.rollback()
                raise
        except Exception as e:
            logger.error(f"Insert error: {e}")
            self.conn.rollback()
            raise
    
    def insert_ticker_mappings(self, news_records: List[Dict[str, Any]]) -> int:
        """
        Insert news-ticker mappings
        
        Args:
            news_records: List of news records with ticker_sentiment
            
        Returns:
            Number of mappings inserted
        """
        if not news_records:
            return 0
        
        if not self.cursor or not self.conn:
            logger.error("Database connection not established")
            return 0
        
        # First, get news IDs for the message_ids
        message_ids = [r.get('message_id', '') for r in news_records]
        placeholders = ','.join(['?' for _ in message_ids])
        
        query = f"""
        SELECT id, message_id 
        FROM news_sentiment_processed 
        WHERE message_id IN ({placeholders})
        """
        
        self.cursor.execute(query, message_ids)
        id_map = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        # Prepare ticker mappings
        sql = """
        INSERT INTO news_ticker_mapping (
            news_id, ticker, relevance_score, 
            ticker_sentiment_score, ticker_sentiment_label
        )
        VALUES (?, ?, ?, ?, ?)
        """
        
        values = []
        for record in news_records:
            message_id = record.get('message_id', '')
            news_id = id_map.get(message_id)
            
            if not news_id:
                continue
            
            data = record.get('data', {})
            ticker_sentiment = data.get('ticker_sentiment', [])
            
            # Debug logging
            if ticker_sentiment:
                logger.debug(f"Processing {len(ticker_sentiment)} tickers for news_id={news_id}")
            
            for ts in ticker_sentiment:
                values.append((
                    news_id,
                    ts.get('ticker', ''),
                    float(ts.get('relevance_score', 0.0)),
                    float(ts.get('ticker_sentiment_score', 0.0)),
                    ts.get('ticker_sentiment_label', '')
                ))
        
        if not values:
            logger.warning(f"No ticker mappings to insert (processed {len(news_records)} news records)")
            return 0
        
        try:
            self.cursor.executemany(sql, values)
            self.conn.commit()
            logger.info(f"Inserted {len(values)} ticker mappings")
            return len(values)
        except Exception as e:
            logger.error(f"Ticker mapping insert error: {e}")
            self.conn.rollback()
            raise
    
    def update_ticker_daily_aggregates(self, date: dt.date) -> int:
        """
        Update ticker_sentiment_daily aggregates for a specific date
        
        Args:
            date: Date to aggregate
            
        Returns:
            Number of tickers updated
        """
        sql = """
        MERGE INTO ticker_sentiment_daily AS target
        USING (
            SELECT 
                ntm.ticker,
                CAST(nsp.time_published AS DATE) as date,
                COUNT(*) as news_count,
                AVG(ntm.ticker_sentiment_score) as avg_sentiment_score,
                SUM(CASE WHEN ntm.ticker_sentiment_label = 'Bullish' THEN 1 ELSE 0 END) as bullish_count,
                SUM(CASE WHEN ntm.ticker_sentiment_label = 'Bearish' THEN 1 ELSE 0 END) as bearish_count,
                SUM(CASE WHEN ntm.ticker_sentiment_label = 'Neutral' THEN 1 ELSE 0 END) as neutral_count,
                MAX(ntm.ticker_sentiment_score) as top_sentiment_score
            FROM news_ticker_mapping ntm
            JOIN news_sentiment_processed nsp ON ntm.news_id = nsp.id
            WHERE CAST(nsp.time_published AS DATE) = ?
            GROUP BY ntm.ticker, CAST(nsp.time_published AS DATE)
        ) AS source
        ON target.ticker = source.ticker AND target.date = source.date
        WHEN MATCHED THEN
            UPDATE SET
                news_count = source.news_count,
                avg_sentiment_score = source.avg_sentiment_score,
                bullish_count = source.bullish_count,
                bearish_count = source.bearish_count,
                neutral_count = source.neutral_count,
                top_sentiment_score = source.top_sentiment_score,
                updated_at = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT (ticker, date, news_count, avg_sentiment_score, 
                    bullish_count, bearish_count, neutral_count, top_sentiment_score)
            VALUES (source.ticker, source.date, source.news_count, source.avg_sentiment_score,
                    source.bullish_count, source.bearish_count, source.neutral_count, source.top_sentiment_score);
        """
        
        if not self.cursor or not self.conn:
            logger.error(" Database connection not established")
            return 0
        
        try:
            self.cursor.execute(sql, (date,))
            rows_affected = self.cursor.rowcount
            self.conn.commit()
            logger.info(f" Updated {rows_affected} ticker daily aggregates for {date}")
            return rows_affected
        except Exception as e:
            logger.error(f" Aggregate update error: {e}")
            self.conn.rollback()
            raise
    
    def _parse_time_published(self, time_str: str) -> datetime:
        """
        Parse Alpha Vantage timestamp format: 20251216T120000
        
        Args:
            time_str: Timestamp string
            
        Returns:
            datetime object (fallback to current time if parse fails)
        """
        if not time_str:
            logger.warning("Empty timestamp, using current time")
            return datetime.now()
        
        try:
            # Format: 20251216T120000
            return datetime.strptime(time_str, '%Y%m%dT%H%M%S')
        except:
            try:
                # Fallback to ISO format
                return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            except:
                logger.warning(f"Could not parse timestamp: {time_str}, using current time")
                return datetime.now()


class NewsProcessor:
    """Main news processing pipeline"""
    
    def __init__(
        self,
        sql_server_config: Dict[str, Any],
        batch_size: int = 100,
        commit_interval_seconds: int = 30
    ):
        """
        Initialize news processor
        
        Args:
            sql_server_config: SQL Server connection config
            batch_size: Batch size for processing
            commit_interval_seconds: Interval for Kafka offset commit
        """
        self.sql_server_config = sql_server_config
        self.batch_size = batch_size
        self.commit_interval_seconds = commit_interval_seconds
        
        self.consumer = None
        self.sql_sink = None
        self.buffer = []
        self.last_commit_time = datetime.now()
        
        # Metrics
        self.messages_consumed = 0
        self.messages_processed = 0
        self.messages_failed = 0
    
    def connect(self):
        """Connect to Kafka and SQL Server"""
        # Kafka
        logger.info(f"Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS}")
        # Create consumer config with specific group_id for SQL processor
        consumer_config = KafkaConfig.CONSUMER_CONFIG.copy()
        consumer_config['group_id'] = 'news-processor-group'
        consumer_config['enable_auto_commit'] = False
        
        self.consumer = KafkaConsumer(
            KafkaConfig.TOPIC_NEWS_SENTIMENT,
            **consumer_config,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info(f" Subscribed to topic: {KafkaConfig.TOPIC_NEWS_SENTIMENT}")
        
        # SQL Server
        self.sql_sink = SQLServerSink(**self.sql_server_config)
        self.sql_sink.connect()
    
    def close(self):
        """Close connections"""
        if self.consumer:
            self.consumer.close()
        if self.sql_sink:
            self.sql_sink.close()
        
        logger.info(
            f" Processor stopped. Consumed: {self.messages_consumed}, "
            f"Processed: {self.messages_processed}, Failed: {self.messages_failed}"
        )
    
    def process_batch(self):
        """Process buffered messages"""
        if not self.buffer:
            return
        
        if not self.sql_sink:
            logger.error(" SQL Server not connected")
            return
        
        logger.info(f"Processing batch of {len(self.buffer)} messages...")
        
        try:
            # Insert news records
            inserted = self.sql_sink.insert_news_sentiment(self.buffer)
            
            # Insert ticker mappings
            if inserted > 0:
                self.sql_sink.insert_ticker_mappings(self.buffer)
            
            # Update daily aggregates
            unique_dates = set()
            for record in self.buffer:
                data = record.get('data', {})
                time_published = self.sql_sink._parse_time_published(
                    data.get('time_published', '')
                )
                if time_published:
                    unique_dates.add(time_published.date())
            
            for date in unique_dates:
                self.sql_sink.update_ticker_daily_aggregates(date)
            
            self.messages_processed += len(self.buffer)
            
        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            self.messages_failed += len(self.buffer)
        finally:
            # Clear buffer
            self.buffer.clear()
            
            # Commit Kafka offset
            if self.consumer:
                self.consumer.commit()
                self.last_commit_time = datetime.now()
                logger.info("Kafka offset committed")
    
    def should_process(self) -> bool:
        """Check if buffer should be processed"""
        if len(self.buffer) >= self.batch_size:
            return True
        
        elapsed = (datetime.now() - self.last_commit_time).total_seconds()
        if elapsed >= self.commit_interval_seconds and len(self.buffer) > 0:
            return True
        
        return False
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting news processor...")
        
        if not self.consumer:
            logger.error(" Kafka consumer not connected. Call connect() first.")
            return
        
        try:
            for message in self.consumer:
                value = message.value
                self.buffer.append(value)
                self.messages_consumed += 1
                
                # Log progress
                if self.messages_consumed % 50 == 0:
                    logger.info(
                        f"Consumed: {self.messages_consumed}, "
                        f"Processed: {self.messages_processed}, "
                        f"Buffered: {len(self.buffer)}"
                    )
                
                # Process if needed
                if self.should_process():
                    self.process_batch()
        
        except KeyboardInterrupt:
            logger.info("\n Interrupted by user")
        except Exception as e:
            logger.error(f"Processor error: {e}")
            raise
        finally:
            # Final processing
            if self.buffer:
                logger.info("Final batch processing...")
                self.process_batch()
            
            self.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='News Processor: Kafka to SQL Server')
    parser.add_argument('--sql-server', default='192.168.0.108', help='SQL Server host')
    parser.add_argument('--sql-port', type=int, default=1433, help='SQL Server port')
    parser.add_argument('--sql-database', default='Streaming', help='Database name')
    parser.add_argument('--sql-user', default="sa", help='SQL Server username (optional for Windows auth)')
    parser.add_argument('--sql-password', default="YourStrong@Password123", help='SQL Server password (optional for Windows auth)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    parser.add_argument('--commit-interval', type=int, default=30, help='Commit interval (seconds)')
    
    args = parser.parse_args()
    
    sql_config = {
        'server': args.sql_server,
        'port': args.sql_port,
        'database': args.sql_database,
        'username': args.sql_user,
        'password': args.sql_password
    }
    
    processor = NewsProcessor(
        sql_server_config=sql_config,
        batch_size=args.batch_size,
        commit_interval_seconds=args.commit_interval
    )
    
    try:
        processor.connect()
        processor.run()
    except Exception as e:
        logger.error(f" Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
