import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from collections import deque

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.window import TumblingEventTimeWindows
    from pyflink.datastream.functions import AggregateFunction
    from pyflink.datastream.connectors.kafka import KafkaSource
    from pyflink.common import Time
    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False

logger = logging.getLogger(__name__)


class OHLCProcessor:
    """
    Process OHLC data stream
    """
    
    def __init__(self):
        self.price_history = {}  # ticker -> deque of prices
        self.max_history = 200  
    
    def process_ohlc(self, ohlc_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            ticker = ohlc_data.get('ticker')
            close_price = float(ohlc_data.get('c', 0))
            
            # Initialize history 
            if ticker not in self.price_history:
                self.price_history[ticker] = deque(maxlen=self.max_history)
            
            self.price_history[ticker].append(close_price)
            
            enriched_data = {
                **ohlc_data,
                'processed_timestamp': datetime.now().isoformat(),
                'technical_indicators': {}
            }
            
            history = list(self.price_history[ticker])
            
            if len(history) >= 20:
                enriched_data['technical_indicators']['sma_20'] = self._calculate_sma(history, 20)
                enriched_data['technical_indicators']['ema_20'] = self._calculate_ema(history, 20)
            
            if len(history) >= 50:
                enriched_data['technical_indicators']['sma_50'] = self._calculate_sma(history, 50)
                enriched_data['technical_indicators']['ema_50'] = self._calculate_ema(history, 50)
            
            if len(history) >= 14:
                enriched_data['technical_indicators']['rsi_14'] = self._calculate_rsi(history, 14)
            
            if len(history) >= 26:
                macd_line, signal_line, histogram = self._calculate_macd(history)
                enriched_data['technical_indicators']['macd'] = {
                    'macd_line': macd_line,
                    'signal_line': signal_line,
                    'histogram': histogram
                }
            
            # Calculate price change 
            if len(history) >= 2:
                prev_price = history[-2]
                price_change = close_price - prev_price
                price_change_pct = (price_change / prev_price) * 100 if prev_price != 0 else 0
                
                enriched_data['price_metrics'] = {
                    'price_change': round(price_change, 2),
                    'price_change_pct': round(price_change_pct, 2),
                    'volatility': self._calculate_volatility(history[-20:]) if len(history) >= 20 else None
                }
            
            # Volume 
            volume = float(ohlc_data.get('v', 0))
            enriched_data['volume_metrics'] = {
                'volume': volume,
                'volume_weighted_price': self._calculate_vwap(ohlc_data)
            }
            
            return enriched_data
            
        except Exception as e:
            logger.error(f"Error processing OHLC: {e}")
            return ohlc_data
    
    def _calculate_sma(self, prices: List[float], period: int) -> Optional[float]:
        if len(prices) < period:
            return None
        return round(sum(prices[-period:]) / period, 2)
    
    def _calculate_ema(self, prices: List[float], period: int) -> Optional[float]:
        if len(prices) < period:
            return None
        
        multiplier = 2 / (period + 1)
        ema = prices[-period]  # Start with SMA
        
        for price in prices[-period+1:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        return round(ema, 2)
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        if len(prices) < period + 1:
            return None
        
        # Calculate price changes
        deltas = [prices[i] - prices[i-1] for i in range(-period, 0)]
        
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return round(rsi, 2)
    
    def _calculate_macd(
        self, 
        prices: List[float], 
        fast_period: int = 12, 
        slow_period: int = 26, 
        signal_period: int = 9
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Calculate MACD (Moving Average Convergence Divergence)"""
        if len(prices) < slow_period:
            return None, None, None
        
        ema_fast = self._calculate_ema(prices, fast_period)
        ema_slow = self._calculate_ema(prices, slow_period)
        
        if ema_fast is None or ema_slow is None:
            return None, None, None
        
        macd_line = ema_fast - ema_slow
        
        # For signal line, would need MACD history (simplified here)
        signal_line = macd_line * 0.9  # Approximation
        histogram = macd_line - signal_line
        
        return round(macd_line, 2), round(signal_line, 2), round(histogram, 2)
    
    def _calculate_volatility(self, prices: List[float]) -> Optional[float]:
        """Calculate price volatility (standard deviation of returns)"""
        if len(prices) < 2:
            return None
        
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
        
        avg_return = sum(returns) / len(returns)
        variance = sum((r - avg_return) ** 2 for r in returns) / len(returns)
        volatility = variance ** 0.5
        
        return round(volatility, 4)
    
    def _calculate_vwap(self, ohlc_data: Dict[str, Any]) -> Optional[float]:
        """Calculate Volume Weighted Average Price for single bar"""
        try:
            high = float(ohlc_data.get('h', 0))
            low = float(ohlc_data.get('l', 0))
            close = float(ohlc_data.get('c', 0))
            volume = float(ohlc_data.get('v', 0))
            
            typical_price = (high + low + close) / 3
            vwap = typical_price * volume
            
            return round(vwap, 2)
        except:
            return None
    
    def aggregate_ohlc_by_ticker(
        self, 
        ticker: str, 
        ohlc_items: List[Dict[str, Any]],
        aggregation_period: str = '1H'
    ) -> Dict[str, Any]:
        if not ohlc_items:
            return {}
        
        # Sort by timestamp
        sorted_items = sorted(ohlc_items, key=lambda x: x.get('t', 0))
        
        # Get first and last items
        first = sorted_items[0]
        last = sorted_items[-1]
        
        # Calculate aggregated OHLC
        opens = [float(item.get('o', 0)) for item in sorted_items]
        highs = [float(item.get('h', 0)) for item in sorted_items]
        lows = [float(item.get('l', 0)) for item in sorted_items]
        closes = [float(item.get('c', 0)) for item in sorted_items]
        volumes = [float(item.get('v', 0)) for item in sorted_items]
        
        return {
            'ticker': ticker,
            'period': aggregation_period,
            'start_timestamp': first.get('t'),
            'end_timestamp': last.get('t'),
            'open': opens[0],
            'high': max(highs),
            'low': min(lows),
            'close': closes[-1],
            'volume': sum(volumes),
            'bar_count': len(sorted_items),
            'avg_price': round(sum(closes) / len(closes), 2),
            'price_change': round(closes[-1] - opens[0], 2),
            'price_change_pct': round(((closes[-1] - opens[0]) / opens[0]) * 100, 2) if opens[0] != 0 else 0,
            'aggregated_at': datetime.now().isoformat()
        }


class OHLCAggregator(AggregateFunction):
    """
    Aggregate OHLC data by ticker and time window
    """
    
    def create_accumulator(self) -> List[Dict[str, Any]]:
        return []
    
    def add(self, value: Dict[str, Any], accumulator: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        accumulator.append(value)
        return accumulator
    
    def get_result(self, accumulator: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not accumulator:
            return {}
        
        # Sort by timestamp
        sorted_items = sorted(accumulator, key=lambda x: x.get('t', 0))
        ticker = sorted_items[0].get('ticker', 'UNKNOWN')
        
        # Extract prices and volumes
        opens = [float(item.get('o', 0)) for item in sorted_items]
        highs = [float(item.get('h', 0)) for item in sorted_items]
        lows = [float(item.get('l', 0)) for item in sorted_items]
        closes = [float(item.get('c', 0)) for item in sorted_items]
        volumes = [float(item.get('v', 0)) for item in sorted_items]
        
        return {
            'ticker': ticker,
            'start_timestamp': sorted_items[0].get('t'),
            'end_timestamp': sorted_items[-1].get('t'),
            'open': opens[0],
            'high': max(highs),
            'low': min(lows),
            'close': closes[-1],
            'volume': sum(volumes),
            'bar_count': len(sorted_items),
            'avg_price': round(sum(closes) / len(closes), 2),
            'aggregated_at': datetime.now().isoformat()
        }
    
    def merge(self, acc_a: List[Dict[str, Any]], acc_b: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return acc_a + acc_b


class MongoDBSink:
    
    def __init__(self, connection_string: Optional[str] = None, database: str = "stock_data", collection: str = "ohlc_aggregated"):
        self.connection_string = connection_string
        self.database = database
        self.collection = collection


def create_ohlc_processing_job():
    
    if not PYFLINK_AVAILABLE:
        logger.error("PyFlink is not available. ")
        return None
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(6)
    
    return env


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test processor
    processor = OHLCProcessor()
    
    # Simulate price data
    for i in range(30):
        test_ohlc = {
            'ticker': 'AAPL',
            't': 1700000000 + (i * 3600),
            'o': 150 + i * 0.5,
            'h': 151 + i * 0.5,
            'l': 149 + i * 0.5,
            'c': 150.5 + i * 0.5,
            'v': 1000000 + i * 10000
        }
        
        processed = processor.process_ohlc(test_ohlc)
    
    print("Last processed OHLC:", json.dumps(processed, indent=2))
    ohlc_items = [test_ohlc for _ in range(5)]
    aggregated = processor.aggregate_ohlc_by_ticker('AAPL', ohlc_items, '1H')
    print("\nAggregated OHLC:", json.dumps(aggregated, indent=2))
