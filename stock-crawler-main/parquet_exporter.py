"""
Batch Export Pipeline: MongoDB -> Parquet Files
Export historical data for ML training and long-term storage
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional, Dict
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetExporter:
    """Export MongoDB data to Parquet files for batch processing"""
    
    def __init__(self, export_dir: str = "./data/parquet"):
        from database.mongodb import MongoDB
        self.mongodb = MongoDB()
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Export directory: {self.export_dir}")
    
    def export_ohlc_data(self, start_date: datetime, end_date: datetime, 
                         partition_by: str = 'day') -> Dict[str, int]:
        """
        Export OHLC data partitioned by date
        
        Args:
            start_date: Start date for export
            end_date: End date for export
            partition_by: Partition granularity ('day', 'week', 'month')
        
        Returns:
            Dict with export statistics
        """
        logger.info(f"Exporting OHLC data from {start_date} to {end_date}")
        
        collection = self.mongodb.get_collection('OHLC_processed')
        
        # Query MongoDB
        query = {
            'processed_at': {
                '$gte': start_date.isoformat(),
                '$lte': end_date.isoformat()
            }
        }
        
        cursor = collection.find(query)
        
        # Convert to DataFrame
        data = []
        for doc in cursor:
            # Extract technical indicators
            tech_indicators = doc.get('technical_indicators', {})
            
            data.append({
                'timestamp': datetime.fromtimestamp(doc.get('t', 0) / 1000),
                'ticker': doc.get('ticker'),
                'open': doc.get('o'),
                'high': doc.get('h'),
                'low': doc.get('l'),
                'close': doc.get('c'),
                'volume': doc.get('v'),
                'vwap': doc.get('vw'),
                'price_range': tech_indicators.get('price_range'),
                'price_range_pct': tech_indicators.get('price_range_pct'),
                'volume_weighted_price': tech_indicators.get('volume_weighted_price'),
                'processed_at': doc.get('processed_at')
            })
        
        if not data:
            logger.warning("No OHLC data found for export")
            return {'files_created': 0, 'rows_exported': 0}
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        
        # Partition and export
        stats = {'files_created': 0, 'rows_exported': 0}
        
        if partition_by == 'day':
            for date, group in df.groupby('date'):
                file_path = self.export_dir / f"ohlc/date={date}/data.parquet"
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Write Parquet file
                group.drop('date', axis=1).to_parquet(
                    file_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
                
                stats['files_created'] += 1
                stats['rows_exported'] += len(group)
                logger.info(f"Exported {len(group)} rows to {file_path}")
        
        elif partition_by == 'week':
            df['week'] = df['timestamp'].dt.isocalendar().week
            df['year'] = df['timestamp'].dt.year
            
            for (year, week), group in df.groupby(['year', 'week']):
                file_path = self.export_dir / f"ohlc/year={year}/week={week}/data.parquet"
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                group.drop(['date', 'week', 'year'], axis=1).to_parquet(
                    file_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
                
                stats['files_created'] += 1
                stats['rows_exported'] += len(group)
        
        elif partition_by == 'month':
            df['month'] = df['timestamp'].dt.month
            df['year'] = df['timestamp'].dt.year
            
            for (year, month), group in df.groupby(['year', 'month']):
                file_path = self.export_dir / f"ohlc/year={year}/month={month:02d}/data.parquet"
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                group.drop(['date', 'month', 'year'], axis=1).to_parquet(
                    file_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
                
                stats['files_created'] += 1
                stats['rows_exported'] += len(group)
        
        logger.info(f"OHLC export complete: {stats}")
        return stats
    
    def export_news_sentiment(self, start_date: datetime, end_date: datetime,
                              partition_by: str = 'day') -> Dict[str, int]:
        """Export news sentiment data partitioned by date"""
        logger.info(f"Exporting news sentiment from {start_date} to {end_date}")
        
        collection = self.mongodb.get_collection('news_sentiment_processed')
        
        query = {
            'processed_at': {
                '$gte': start_date.isoformat(),
                '$lte': end_date.isoformat()
            }
        }
        
        cursor = collection.find(query)
        
        data = []
        for doc in cursor:
            sentiment_summary = doc.get('sentiment_summary', {})
            
            # Parse timestamp
            time_val = doc.get('timestamp')
            if isinstance(time_val, str):
                try:
                    timestamp = datetime.fromisoformat(time_val.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcnow()
            
            data.append({
                'timestamp': timestamp,
                'ticker': doc.get('ticker'),
                'title': doc.get('title'),
                'url': doc.get('url'),
                'sentiment_score': doc.get('sentiment_score'),
                'sentiment_label': doc.get('sentiment_label'),
                'overall_sentiment': sentiment_summary.get('overall_sentiment'),
                'source': doc.get('source'),
                'processed_at': doc.get('processed_at')
            })
        
        if not data:
            logger.warning("No news sentiment data found for export")
            return {'files_created': 0, 'rows_exported': 0}
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        
        stats = {'files_created': 0, 'rows_exported': 0}
        
        for date, group in df.groupby('date'):
            file_path = self.export_dir / f"news_sentiment/date={date}/data.parquet"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            group.drop('date', axis=1).to_parquet(
                file_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            stats['files_created'] += 1
            stats['rows_exported'] += len(group)
            logger.info(f"Exported {len(group)} rows to {file_path}")
        
        logger.info(f"News sentiment export complete: {stats}")
        return stats
    
    def export_all(self, days_back: int = 30, partition_by: str = 'day'):
        """Export all data types for the last N days"""
        logger.info("="*60)
        logger.info(f"Starting full export (last {days_back} days)")
        logger.info("="*60)
        
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        ohlc_stats = self.export_ohlc_data(start_date, end_date, partition_by)
        news_stats = self.export_news_sentiment(start_date, end_date, partition_by)
        
        logger.info("="*60)
        logger.info("Export Summary:")
        logger.info(f"  OHLC: {ohlc_stats['files_created']} files, {ohlc_stats['rows_exported']} rows")
        logger.info(f"  News: {news_stats['files_created']} files, {news_stats['rows_exported']} rows")
        logger.info("="*60)
    
    def read_parquet_files(self, data_type: str, date_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Read Parquet files back into DataFrame
        
        Args:
            data_type: 'ohlc' or 'news_sentiment'
            date_filter: Optional date filter (e.g., 'date=2025-11-27')
        
        Returns:
            Pandas DataFrame
        """
        base_path = self.export_dir / data_type
        
        if date_filter:
            path = base_path / date_filter
        else:
            path = base_path
        
        if not path.exists():
            logger.warning(f"Path not found: {path}")
            return pd.DataFrame()
        
        # Read all parquet files in directory
        df = pd.read_parquet(path, engine='pyarrow')
        logger.info(f"Loaded {len(df)} rows from {path}")
        return df


def main():
    """Main function for scheduled export"""
    import sys
    
    days_back = 30
    partition_by = 'day'
    
    if len(sys.argv) > 1:
        try:
            days_back = int(sys.argv[1])
        except:
            pass
    
    if len(sys.argv) > 2:
        partition_by = sys.argv[2]
    
    exporter = ParquetExporter()
    exporter.export_all(days_back=days_back, partition_by=partition_by)


if __name__ == "__main__":
    main()
