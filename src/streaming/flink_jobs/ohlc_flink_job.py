import json
import logging
from datetime import datetime
from typing import Dict, Any

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
    )
    from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy
    from pyflink.datastream.functions import MapFunction, ProcessFunction
    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False
    print("PyFlink not installed.")

from config.kafka_config import KafkaConfig, FlinkConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OHLCEnrichmentFunction(MapFunction):
    """
    Enrich OHLC data
    """
    
    def map(self, value: str) -> str:
        try:
            data = json.loads(value)
            
            # Extract OHLC values
            ticker = data.get('ticker', 'UNKNOWN')
            close_price = float(data.get('c', 0))
            high_price = float(data.get('h', 0))
            low_price = float(data.get('l', 0))
            open_price = float(data.get('o', 0))
            volume = float(data.get('v', 0))
            timestamp = data.get('t', 0)
            
            price_range = high_price - low_price
            price_change = close_price - open_price
            
            enriched_data = {
                **data,
                'processed_timestamp': datetime.now().isoformat(),
                'processor': 'pyflink',
                'technical_indicators': {
                    'price_range': round(price_range, 2),
                    'price_range_pct': round((price_range / close_price * 100), 2) if close_price > 0 else 0,
                    'price_change': round(price_change, 2),
                    'price_change_pct': round((price_change / open_price * 100), 2) if open_price > 0 else 0,
                    'volume': volume,
                    'volatility_indicator': 'high' if (price_range / close_price * 100) > 5 else 'normal',
                    'trend': 'bullish' if price_change > 0 else 'bearish' if price_change < 0 else 'neutral'
                },
                'metadata': {
                    'ticker': ticker,
                    'timestamp': timestamp,
                    'processing_time': datetime.now().isoformat()
                }
            }
            
            logger.info(
                f"Processed OHLC: {ticker} | "
                f"Close: ${close_price:.2f} | "
                f"Change: {enriched_data['technical_indicators']['price_change_pct']:.2f}% | "
                f"Trend: {enriched_data['technical_indicators']['trend']}"
            )
            
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Error enriching OHLC data: {e}")
            return value


class PriceAlertFunction(ProcessFunction):
    """
    Alert on significant price
    """
    
    def process_element(self, value: str, ctx: ProcessFunction.Context):
        try:
            data = json.loads(value)
            indicators = data.get('technical_indicators', {})
            
            # Check change (>3%)
            price_change_pct = abs(indicators.get('price_change_pct', 0))
            
            if price_change_pct > 3:
                alert = {
                    'alert_type': 'SIGNIFICANT_PRICE_CHANGE',
                    'ticker': data.get('ticker'),
                    'price_change_pct': price_change_pct,
                    'close_price': data.get('c'),
                    'timestamp': datetime.now().isoformat(),
                    'message': f"Alert: {data.get('ticker')} moved {price_change_pct:.2f}%"
                }
                
                logger.warning(f"ALERT: {alert['message']}")
                
                yield data
                yield alert
            else:
                yield data
                
        except Exception as e:
            logger.error(f"Error in alert function: {e}")
            yield json.loads(value) if isinstance(value, str) else value


def create_ohlc_flink_job():
    
    if not PYFLINK_AVAILABLE:
        logger.error("PyFlink is not available.")
        return None
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FlinkConfig.DEFAULT_PARALLELISM)
    env.enable_checkpointing(FlinkConfig.CHECKPOINT_INTERVAL_MS)
    
    logger.info("PyFlink OHLC Processing Job")
    logger.info(f"Kafka Bootstrap: {KafkaConfig.BOOTSTRAP_SERVERS}")
    logger.info(f"Input Topic: {KafkaConfig.TOPIC_OHLC_DATA}")
    logger.info(f"Parallelism: {FlinkConfig.DEFAULT_PARALLELISM}")
    
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS) \
        .set_topics(KafkaConfig.TOPIC_OHLC_DATA) \
        .set_group_id('flink-ohlc-processor') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create data stream
    ohlc_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "OHLC Kafka Source"
    )
    
    # Apply transformations
    enriched_stream = ohlc_stream \
        .map(OHLCEnrichmentFunction()) \
        .name("Enrich OHLC Data")
    
    alert_stream = enriched_stream \
        .process(PriceAlertFunction()) \
        .name("Price Alert Detection")
    
    alert_stream.print()
    
    # Configure Kafka sink (output back to Kafka)
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("stock-ohlc-processed")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    
    # Sink to Kafka
    alert_stream.sink_to(kafka_sink).name("Kafka Sink - Processed OHLC")
    
    return env


def run_ohlc_job():
    env = create_ohlc_flink_job()
    
    if env:
        logger.info("Starting PyFlink OHLC Processing Job...")
        
        try:
            env.execute("Stock OHLC Processing")
        except KeyboardInterrupt:
            logger.info("\nJob interrupted")
        except Exception as e:
            logger.error(f"Job execution error: {e}")
    else:
        logger.error("Failed to create Flink job")


if __name__ == "__main__":
    run_ohlc_job()
