import os

# Load .env file if available (optional for containers)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Skip if dotenv not installed


class MongoDBConfig:
    """MongoDB database configuration"""
    CONNECTION_URL = os.environ.get("MONGO_CONNECTION_URL") or "mongodb://localhost:27017"
    DATABASE = os.environ.get("MONGO_DATABASE") or "stock_analytics"


class PolygonConfig:
    """Polygon.io API configuration"""
    API_KEY = os.environ.get('POLYGON_API_KEY') or "abc"


class AlphavantageConfig:
    """Alpha Vantage API configuration"""
    API_KEY = os.environ.get('ALPHAVANTAGE_API_KEY') or "abc"


class RedisConfig:
    """Redis cache configuration"""
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


if __name__ == "__main__":
    print("Configuration:")
    print(f"  MongoDB: {MongoDBConfig.CONNECTION_URL}")
    print(f"  Database: {MongoDBConfig.DATABASE}")
    print(f"  Polygon API Key: {'***' if PolygonConfig.API_KEY != 'abc' else 'Not set'}")
    print(f"  Alpha Vantage API Key: {'***' if AlphavantageConfig.API_KEY != 'abc' else 'Not set'}")
    print(f"  Redis: {RedisConfig.HOST}:{RedisConfig.PORT}")
