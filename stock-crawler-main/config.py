import os
from dotenv import load_dotenv

load_dotenv()


class MongoDBConfig:
    CONNECTION_URL = os.environ.get("MONGO_CONNECTION_URL") or "mongodb://localhost:27017"
    DATABASE = os.environ.get("MONGO_DATABASE") or "test"

class PolygonConfig:
    API_KEY = os.environ.get('POLYGON_API_KEY') or "abc"

class AlphavantageConfig:
    API_KEY = os.environ.get('ALPHAVANTAGE_API_KEY') or "abc"

if __name__ == "__main__":
    print(MongoDBConfig.CONNECTION_URL)