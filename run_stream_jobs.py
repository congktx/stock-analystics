"""
Stream Job Manager
Manage and run Kafka consumers and PyFlink jobs
"""
import logging
import sys
import argparse
from typing import Optional, Dict, Tuple, Callable, Any, Union

from src.streaming.kafka_consumer import (
    create_ohlc_consumer,
    create_news_consumer,
    create_company_consumer,
    create_market_consumer
)

try:
    from src.streaming.flink_jobs.ohlc_flink_job import run_ohlc_job
    from src.streaming.flink_jobs.news_flink_job import run_news_job
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False
    print("⚠️  PyFlink not available. Only Kafka consumers will be available.")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamJobManager:
    """
    Manager for stream processing jobs
    """
    
    def __init__(self):
        # Type: Dict[job_type, Dict[job_name, Tuple[title, function]]]
        self.jobs: Dict[str, Dict[str, Tuple[str, Callable[[], Any]]]] = {
            'kafka': {
                'ohlc': ('OHLC Kafka Consumer', create_ohlc_consumer),
                'news': ('News Kafka Consumer', create_news_consumer),
                'company': ('Company Kafka Consumer', create_company_consumer),
                'market': ('Market Kafka Consumer', create_market_consumer),
            }
        }
        
        if FLINK_AVAILABLE:
            self.jobs['flink'] = {
                'ohlc': ('OHLC PyFlink Job', run_ohlc_job),
                'news': ('News PyFlink Job', run_news_job),
            }
    
    def list_jobs(self):
        """List all available jobs"""
        print("\n" + "="*60)
        print("Available Stream Processing Jobs")
        print("="*60)
        
        print("\n📨 Kafka Consumers:")
        for key, (name, _) in self.jobs['kafka'].items():
            print(f"  - kafka:{key} - {name}")
        
        if FLINK_AVAILABLE and 'flink' in self.jobs:
            print("\n🚀 PyFlink Jobs:")
            for key, (name, _) in self.jobs['flink'].items():
                print(f"  - flink:{key} - {name}")
        else:
            print("\n⚠️  PyFlink not available. Install with: pip install apache-flink")
        
        print("\n" + "="*60)
    
    def run_job(self, job_type: str, job_name: str):
        """
        Run a specific job
        
        Args:
            job_type: 'kafka' or 'flink'
            job_name: Job name (e.g., 'ohlc', 'news')
        """
        if job_type not in self.jobs:
            logger.error(f"Unknown job type: {job_type}")
            logger.info(f"Available types: {', '.join(self.jobs.keys())}")
            return
        
        if job_name not in self.jobs[job_type]:
            logger.error(f"Unknown job: {job_name}")
            logger.info(f"Available {job_type} jobs: {', '.join(self.jobs[job_type].keys())}")
            return
        
        job_title, job_func = self.jobs[job_type][job_name]
        
        logger.info("="*60)
        logger.info(f"Starting: {job_title}")
        logger.info("="*60)
        
        try:
            if job_type == 'kafka':
                consumer = job_func()
                consumer.start()
            else:  # flink
                job_func()
        except KeyboardInterrupt:
            logger.info("\n⚠️  Job interrupted by user")
        except Exception as e:
            logger.error(f"Job execution error: {e}")
    
    def run_all_consumers(self):
        """Run all Kafka consumers (for testing)"""
        import multiprocessing
        
        logger.info("Starting all Kafka consumers in parallel...")
        
        processes = []
        for job_name, (job_title, job_func) in self.jobs['kafka'].items():
            logger.info(f"Starting: {job_title}")
            consumer = job_func()
            p = multiprocessing.Process(target=consumer.start)
            p.start()
            processes.append(p)
        
        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            logger.info("\n⚠️  Stopping all consumers...")
            for p in processes:
                p.terminate()


def main():
    """
    Main entry point for stream job manager
    """
    parser = argparse.ArgumentParser(
        description='Stock Analytics Stream Job Manager',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all jobs
  python run_stream_jobs.py --list

  # Run Kafka consumer for OHLC data
  python run_stream_jobs.py kafka ohlc

  # Run PyFlink job for news sentiment
  python run_stream_jobs.py flink news

  # Run all Kafka consumers
  python run_stream_jobs.py --all
        """
    )
    
    parser.add_argument(
        'job_type',
        nargs='?',
        choices=['kafka', 'flink'],
        help='Type of job to run (kafka or flink)'
    )
    
    parser.add_argument(
        'job_name',
        nargs='?',
        help='Name of job to run (ohlc, news, company, market)'
    )
    
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available jobs'
    )
    
    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Run all Kafka consumers'
    )
    
    args = parser.parse_args()
    
    manager = StreamJobManager()
    
    if args.list:
        manager.list_jobs()
    elif args.all:
        manager.run_all_consumers()
    elif args.job_type and args.job_name:
        manager.run_job(args.job_type, args.job_name)
    else:
        parser.print_help()
        print("\n")
        manager.list_jobs()


if __name__ == "__main__":
    main()
