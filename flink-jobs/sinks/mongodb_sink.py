import logging
from typing import Dict, Any, List, Optional
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from pymongo.collection import Collection

logger = logging.getLogger(__name__)

class MongoDBSink:
    """
    Flink sink for writing to MongoDB
    Supports batch writes for better performance
    """

    def __init__(self, connection_url: str, database: str, collection: str, batch_size: int = 100):
        """
        Initialize MongoDB sink

        Args:
            connection_url: MongoDB connection URL
            database: Database name
            collection: Collection name
            batch_size: Number of documents to batch before writing
        """
        self.connection_url = connection_url
        self.database_name = database
        self.collection_name = collection
        self.batch_size = batch_size

        self.client: Optional[MongoClient] = None
        self.db = None
        self.collection: Optional[Collection] = None
        self.batch: List[Dict[str, Any]] = []
        self.write_count = 0
        self.error_count = 0

    def open(self):
        """Open connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_url)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            logger.info(f"✓ MongoDB sink opened: {self.collection_name}")
        except Exception as e:
            logger.error(f"✗ Failed to open MongoDB connection: {e}")
            raise
    def write(self, document: Dict[str, Any]):
        """
        Write a single document (buffered)
        
        Args:
            document: Document to write
        """
        self.batch.append(document)
        
        if len(self.batch) >= self.batch_size:
            self.flush()
            
    def flush(self):
        """
        Flush buffered documents to MongoDB
        """

        if not self.batch or self.collection is None:
            return
        
        try:
            # Prepare bulk upsert operations
            bulk_ops = []
            for doc in self.batch:
                if '_id' in doc:
                    bulk_ops.append(
                        UpdateOne(
                            {'_id': doc['_id']},
                            {'$set': doc},
                            upsert=True
                        )
                    )
                else:
                    # If no _id, insert directly
                    self.collection.insert_one(doc)
            
            if bulk_ops:
                result = self.collection.bulk_write(bulk_ops, ordered=False)
                self.write_count += result.upserted_count + result.modified_count
            
            logger.debug(f"✓ Flushed {len(self.batch)} documents to {self.collection_name}")
            self.batch.clear()
        
        except BulkWriteError as e:
            logger.error(f"✗ Bulk write error: {e.details}")
            self.error_count += len(e.details.get('writeErrors', []))
            self.batch.clear()
        except Exception as e:
            logger.error(f"✗ Error flushing to MongoDB: {e}")
            self.error_count += len(self.batch)
            self.batch.clear()
    
    def close(self):
        """Close MongoDB connection"""

        try:
            self.flush()
            if self.client:
                self.client.close()
            
            logger.info(
                f"✓ MongoDB sink closed. "
                f"Written: {self.write_count}, Errors: {self.error_count}"
            )
        except Exception as e:
            logger.error(f"✗ Error closing MongoDB sink: {e}")

    def get_stats(self) -> Dict[str, int]:
        """Get sink statistics"""

        return {
            'documents_written': self.write_count,
            'errors': self.error_count,
            'pedding_batch': len(self.batch)
        }

class MongoDBBatchSink:
    """
    Batch-optimized MongoDB sink for high-throughput senarios
    """

    def __init__(self, connection_url: str, database: str, collections: Dict[str, str]):
        """
        Initialize multi-collection MongoDB sink

        Args: connection_url: MongoDB connection URL
        database: Database name
        collections: Dict mapping data type to collection names
        """
        self.connection_url = connection_url
        self.database_name = database
        self.collection_names = collections

        self.client = None
        self.db = None
        self.collections = {}
        self.sinks = {}

    def open(self):
        """Open connections to MongoDB"""
        try:
            self.client = MongoClient(self.connection_url)
            self.db = self.client[self.database_name]

            for data_type, collection_name in self.collection_names.items():
                self.collections[data_type] = self.db[collection_name]
                self.sinks[data_type] = MongoDBSink(
                    self.connection_url,
                    self.database_name,
                    collection_name
                )
                self.sinks[data_type].open()
            logger.info(f"✓ Batch MongoDB sink opened for {len(self.collections)} collections")
        except Exception as e:
            logger.error(f"✗ Failed to open batch MongoDB connection: {e}")
            raise
    def write(self, data_type: str, document: Dict[str, Any]):
        """
        Write document to specific collection

        Args:
            data_type: Type of data (determones collection)
            document: Document to write
        """
        if data_type in self.sinks:
            self.sinks[data_type].write(document)
        else:
            logger.warning(f"⚠️  Unknown data type: {data_type}")
    
    def flush_all(self):
        """Flush all sinks"""
        for sink in self.sinks.values():
            sink.close()
        if self.client:
            self.client.close()
    
    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Get statistics for all sinks"""
        return {
            data_type: sink.get_stats()
            for data_type, sink in self.sinks.items()
        }
    
if __name__ == "__main__":
    # Test MongoDB sink
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logging.basicConfig(level=logging.INFO)
    
    connection_url = os.environ.get("MONGO_CONNECTION_URL", "mongodb://localhost:27017")
    
    # Test single sink
    sink = MongoDBSink(connection_url, "test_db", "test_collection", batch_size=5)
    sink.open()
    
    # Write test documents
    for i in range(12):
        sink.write({
            "_id": f"test_{i}",
            "value": i * 10,
            "type": "test"
        })
    
    sink.close()
    print(f"Stats: {sink.get_stats()}")
