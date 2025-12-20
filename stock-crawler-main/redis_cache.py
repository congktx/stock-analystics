"""
Redis Cache Manager for Stock Analytics
Real-time caching and state management
"""
import redis
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedisConfig:
    """Redis connection configuration"""
    HOST = os.getenv('REDIS_HOST', 'localhost')
    PORT = int(os.getenv('REDIS_PORT', '6379'))
    DB = int(os.getenv('REDIS_DB', '0'))
    PASSWORD = os.getenv('REDIS_PASSWORD', None)
    
    # TTL settings (in seconds)
    TTL_LATEST_PRICE = 300  # 5 minutes
    TTL_SENTIMENT = 3600  # 1 hour
    TTL_AGGREGATION_5MIN = 300
    TTL_AGGREGATION_15MIN = 900
    TTL_AGGREGATION_1H = 3600
    TTL_LEADERBOARD = 60  # 1 minute


class RedisCache:
    """Redis cache manager for stock data"""
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host=RedisConfig.HOST,
            port=RedisConfig.PORT,
            db=RedisConfig.DB,
            password=RedisConfig.PASSWORD,
            decode_responses=True
        )
        logger.info(f"Connected to Redis at {RedisConfig.HOST}:{RedisConfig.PORT}")
    
    # ========== Latest Price Cache ==========
    
    def cache_latest_price(self, ticker: str, data: Dict):
        """Cache latest price for a ticker"""
        key = f"price:latest:{ticker}"
        self.redis_client.setex(
            key,
            RedisConfig.TTL_LATEST_PRICE,
            json.dumps(data)
        )
    
    def get_latest_price(self, ticker: str) -> Optional[Dict]:
        """Get cached latest price"""
        key = f"price:latest:{ticker}"
        data = self.redis_client.get(key)
        if data and isinstance(data, str):
            return json.loads(data)
        return None
    
    def cache_batch_prices(self, prices: Dict[str, Dict]):
        """Cache multiple prices at once"""
        pipe = self.redis_client.pipeline()
        for ticker, data in prices.items():
            key = f"price:latest:{ticker}"
            pipe.setex(key, RedisConfig.TTL_LATEST_PRICE, json.dumps(data))
        pipe.execute()
        logger.info(f"Cached {len(prices)} prices")
    
    # ========== Sentiment Cache ==========
    
    def cache_sentiment(self, ticker: str, sentiment_data: Dict):
        """Cache latest sentiment for ticker"""
        key = f"sentiment:latest:{ticker}"
        self.redis_client.setex(
            key,
            RedisConfig.TTL_SENTIMENT,
            json.dumps(sentiment_data)
        )
    
    def get_sentiment(self, ticker: str) -> Optional[Dict]:
        """Get cached sentiment"""
        key = f"sentiment:latest:{ticker}"
        data = self.redis_client.get(key)
        if data and isinstance(data, str):
            return json.loads(data)
        return None
    
    # ========== Time Window Aggregations ==========
    
    def update_price_window(self, ticker: str, window: str, price_data: Dict):
        """Update sliding window aggregation (5min, 15min, 1h)"""
        key = f"price:window:{window}:{ticker}"
        
        # Store as sorted set with timestamp as score
        timestamp = price_data.get('timestamp', datetime.now(timezone.utc).timestamp())
        self.redis_client.zadd(key, {json.dumps(price_data): timestamp})
        
        # Set TTL based on window
        ttl_map = {
            '5min': RedisConfig.TTL_AGGREGATION_5MIN,
            '15min': RedisConfig.TTL_AGGREGATION_15MIN,
            '1h': RedisConfig.TTL_AGGREGATION_1H
        }
        self.redis_client.expire(key, ttl_map.get(window, 3600))
        
        # Remove old entries (keep only data within window)
        window_seconds = {
            '5min': 300,
            '15min': 900,
            '1h': 3600
        }
        cutoff = timestamp - window_seconds.get(window, 3600)
        self.redis_client.zremrangebyscore(key, '-inf', cutoff)
    
    def get_price_window(self, ticker: str, window: str) -> List[Dict]:
        """Get all data in time window"""
        key = f"price:window:{window}:{ticker}"
        data = self.redis_client.zrange(key, 0, -1)  
        if data is None:
            return []
        return [json.loads(item) for item in list(data) if isinstance(item, str)]  # type: ignore
    
    def compute_window_stats(self, ticker: str, window: str) -> Optional[Dict]:
        """Compute statistics for time window"""
        data = self.get_price_window(ticker, window)
        if not data:
            return None
        
        prices = [d.get('close', 0) for d in data if 'close' in d]
        volumes = [d.get('volume', 0) for d in data if 'volume' in d]
        
        if not prices:
            return None
        
        return {
            'ticker': ticker,
            'window': window,
            'avg_price': sum(prices) / len(prices),
            'max_price': max(prices),
            'min_price': min(prices),
            'price_change': prices[-1] - prices[0] if len(prices) > 1 else 0,
            'price_change_pct': ((prices[-1] - prices[0]) / prices[0] * 100) if len(prices) > 1 and prices[0] != 0 else 0,
            'total_volume': sum(volumes),
            'data_points': len(data),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    # ========== Leaderboards ==========
    
    def update_leaderboard(self, leaderboard_type: str, ticker: str, value: float):
        """Update leaderboard (top gainers, losers, volume, etc)"""
        key = f"leaderboard:{leaderboard_type}"
        self.redis_client.zadd(key, {ticker: value})
        self.redis_client.expire(key, RedisConfig.TTL_LEADERBOARD)
    
    def get_top_gainers(self, limit: int = 10) -> List[Dict]:
        """Get top gaining stocks"""
        key = "leaderboard:gainers"
        data = self.redis_client.zrevrange(key, 0, limit - 1, withscores=True)  
        if data is None:
            return []
        return [{'ticker': ticker, 'change_pct': score} for ticker, score in list(data)] # type: ignore
    
    def get_top_losers(self, limit: int = 10) -> List[Dict]:
        """Get top losing stocks"""
        key = "leaderboard:losers"
        data = self.redis_client.zrange(key, 0, limit - 1, withscores=True)  
        return [{'ticker': ticker, 'change_pct': score} for ticker, score in data] # type: ignore
    
    def get_top_volume(self, limit: int = 10) -> List[Dict]:
        """Get stocks with highest volume"""
        key = "leaderboard:volume"
        data = self.redis_client.zrevrange(key, 0, limit - 1, withscores=True)  
        if data is None:
            return []
        return [{'ticker': ticker, 'volume': score} for ticker, score in list(data)] # type: ignore
    
    # ========== Pub/Sub for Real-time Alerts ==========
    
    def publish_alert(self, alert_type: str, data: Dict):
        """Publish alert to channel"""
        channel = f"alerts:{alert_type}"
        self.redis_client.publish(channel, json.dumps(data))
        logger.info(f"Published alert to {channel}: {data}")
    
    def subscribe_alerts(self, alert_types: List[str], callback):
        """Subscribe to alert channels"""
        pubsub = self.redis_client.pubsub()
        channels = [f"alerts:{t}" for t in alert_types]
        pubsub.subscribe(*channels)
        
        logger.info(f"Subscribed to channels: {channels}")
        for message in pubsub.listen():
            if message['type'] == 'message':
                msg_data = message.get('data')
                if msg_data and isinstance(msg_data, str):
                    data = json.loads(msg_data)
                    callback(message['channel'], data)
    
    # ========== Health Check ==========
    
    def health_check(self) -> Dict:
        """Check Redis health and stats"""
        try:
            info = self.redis_client.info()  # type: ignore
            if not isinstance(info, dict):
                info = {}
            return {
                'status': 'healthy',
                'connected_clients': info.get('connected_clients', 0),
                'used_memory_human': info.get('used_memory_human', 'N/A'),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace': self.redis_client.dbsize()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def close(self):
        """Close Redis connection"""
        self.redis_client.close()
        logger.info("Redis connection closed")


# Example usage
if __name__ == "__main__":
    cache = RedisCache()
    
    # Test cache operations
    print("\n" + "="*60)
    print("Redis Cache Test")
    print("="*60)
    
    # Cache latest price
    cache.cache_latest_price('AAPL', {
        'ticker': 'AAPL',
        'close': 175.50,
        'volume': 50000000,
        'timestamp': datetime.now(timezone.utc).isoformat()
    })
    
    # Retrieve price
    price = cache.get_latest_price('AAPL')
    print(f"\nCached price for AAPL: {price}")
    
    # Update leaderboards
    cache.update_leaderboard('gainers', 'AAPL', 5.2)
    cache.update_leaderboard('gainers', 'GOOGL', 3.8)
    cache.update_leaderboard('gainers', 'MSFT', 4.1)
    
    # Get top gainers
    gainers = cache.get_top_gainers(3)
    print(f"\nTop gainers: {gainers}")
    
    # Health check
    health = cache.health_check()
    print(f"\nRedis health: {health}")
    
    cache.close()
