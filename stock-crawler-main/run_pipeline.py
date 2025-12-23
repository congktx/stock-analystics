"""
Run Complete End-to-End Pipeline
1. Stream data from MongoDB to Kafka
2. Process data with Kafka consumers
3. Monitor results
"""
import subprocess
import time
import logging
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_data_streamer():
    """Run data streamer in background"""
    logger.info("Starting data streamer...")
    subprocess.Popen(['python', 'test_real_streaming.py'])


def run_ohlc_processor():
    """Run OHLC processor"""
    logger.info("Starting OHLC processor...")
    subprocess.run(['python', 'kafka_consumer_processor.py'], input=b'1\n')


def run_news_processor():
    """Run news processor"""
    logger.info("Starting news processor...")
    subprocess.run(['python', 'kafka_consumer_processor.py'], input=b'2\n')


def verify_results():
    """Verify processed data in MongoDB"""
    logger.info("\nVerifying processed data in MongoDB...")
    
    from database.mongodb import MongoDB
    db = MongoDB()
    
    # Check OHLC processed
    ohlc_processed = db.get_collection('OHLC_processed')
    ohlc_count = ohlc_processed.count_documents({})
    logger.info(f"✓ OHLC processed records: {ohlc_count}")
    
    if ohlc_count > 0:
        sample = ohlc_processed.find_one({})
        if sample:
            logger.info(f"  Sample: Ticker={sample.get('ticker')}, Close=${sample.get('c')}")
            if 'technical_indicators' in sample:
                logger.info(f"  Technical indicators: {sample['technical_indicators']}")
    
    # Check news processed
    news_processed = db.get_collection('news_sentiment_processed')
    news_count = news_processed.count_documents({})
    logger.info(f"✓ News processed records: {news_count}")
    
    if news_count > 0:
        sample = news_processed.find_one({})
        if sample:
            logger.info(f"  Sample: {sample.get('title', 'No title')[:50]}...")
            if 'sentiment_summary' in sample:
                logger.info(f"  Sentiment: {sample['sentiment_summary']['overall_sentiment']}")


def main():
    """Main pipeline orchestrator"""
    logger.info("=" * 70)
    logger.info("COMPLETE END-TO-END STOCK ANALYTICS PIPELINE")
    logger.info("=" * 70)
    
    print("\nPipeline Steps:")
    print("1. Full E2E Test (Stream + Process + Verify)")
    print("2. Stream data only (MongoDB → Kafka)")
    print("3. Process OHLC data only (Kafka → MongoDB)")
    print("4. Process News data only (Kafka → MongoDB)")
    print("5. Verify results in MongoDB")
    print("6. Monitor Flink UI")
    print("7. Exit")
    
    choice = input("\nSelect option (1-7): ").strip()
    
    if choice == '1':
        logger.info("\n" + "=" * 70)
        logger.info("Running Full E2E Pipeline")
        logger.info("=" * 70)
        
        # Step 1: Stream data
        logger.info("\n[Step 1/3] Streaming data from MongoDB to Kafka...")
        from test_real_streaming import RealDataStreamer
        streamer = RealDataStreamer()
        tickers = streamer.list_available_tickers(limit=3)
        if tickers:
            streamer.stream_ohlc_to_kafka(ticker=tickers[0], batch_size=5, interval=1)
        streamer.stream_news_to_kafka(batch_size=5, interval=1)
        streamer.close()
        
        logger.info("\n✓ Data streaming completed")
        time.sleep(2)
        
        # Step 2: Process data (run in separate processes)
        logger.info("\n[Step 2/3] Processing data...")
        logger.info("Starting processors (will run for 10 seconds)...")
        
        # Start OHLC processor in thread
        from kafka_consumer_processor import StockDataProcessor
        processor_ohlc = StockDataProcessor()
        processor_news = StockDataProcessor()
        
        def process_ohlc_limited():
            consumer = processor_ohlc.start_ohlc_consumer()
        
        def process_news_limited():
            consumer = processor_news.start_news_consumer()
        
        # Run processors briefly
        logger.info("Processing OHLC data...")
        subprocess.run(
            ['python', '-c', 
             'from kafka_consumer_processor import StockDataProcessor; '
             'import time; '
             'p = StockDataProcessor(); '
             'import threading; '
             't = threading.Thread(target=p.start_ohlc_consumer); '
             't.daemon = True; '
             't.start(); '
             'time.sleep(5); '
             'p.running = False; '
             'time.sleep(1)'],
            timeout=10,
            capture_output=False
        )
        
        logger.info("Processing News data...")
        subprocess.run(
            ['python', '-c',
             'from kafka_consumer_processor import StockDataProcessor; '
             'import time; '
             'p = StockDataProcessor(); '
             'import threading; '
             't = threading.Thread(target=p.start_news_consumer); '
             't.daemon = True; '
             't.start(); '
             'time.sleep(5); '
             'p.running = False; '
             'time.sleep(1)'],
            timeout=10,
            capture_output=False
        )
        
        logger.info("\n✓ Data processing completed")
        time.sleep(2)
        
        # Step 3: Verify results
        logger.info("\n[Step 3/3] Verifying results...")
        verify_results()
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 70)
        
    elif choice == '2':
        logger.info("\nStreaming data...")
        subprocess.run(['python', 'test_real_streaming.py'])
        
    elif choice == '3':
        logger.info("\nProcessing OHLC data (Press Ctrl+C to stop)...")
        subprocess.run(['python', 'kafka_consumer_processor.py'])
        
    elif choice == '4':
        logger.info("\nProcessing News data (Press Ctrl+C to stop)...")
        subprocess.run(['python', 'kafka_consumer_processor.py'])
        
    elif choice == '5':
        verify_results()
        
    elif choice == '6':
        logger.info("\nOpening Flink UI...")
        logger.info("Access at: http://localhost:8081")
        subprocess.run([
            'kubectl', 'port-forward',
            '-n', 'stock-analytics',
            'svc/flink-jobmanager',
            '8081:8081'
        ])
        
    elif choice == '7':
        logger.info("Exiting...")
    else:
        logger.error("Invalid option")


if __name__ == "__main__":
    main()
