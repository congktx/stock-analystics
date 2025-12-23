import os
from dotenv import load_dotenv

load_dotenv()

class MongoDBConfig:
    CONNECTION_URL = os.environ.get("MONGO_CONNECTION_URL") or "mongodb://localhost:27017"
    DATABASE = os.environ.get("MONGO_DATABASE") or "test"
    
if __name__ == "__main__":
    print(MongoDBConfig.CONNECTION_URL)