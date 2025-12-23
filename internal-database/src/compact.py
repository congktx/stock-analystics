from pymongo import MongoClient
from datetime import datetime
from utils.config import GlobalConfig

def main():
    client = None
    try:
        client = MongoClient(GlobalConfig.MONGO_URI, serverSelectionTimeoutMS=2000)
        # Trigger a connection to verify
        client.server_info()
        db = client["stock-analystics"]
        col = db["company_infos"]
        
        timestamp_31_oct_2024 = int(datetime(2024, 10, 31, 0, 0, 0).timestamp())

        print(f"Timestamp ngày 31-10-2024: {timestamp_31_oct_2024}")

        documents_to_update = col.find({"time_update": {"$gt": timestamp_31_oct_2024}})

        count = 0
        # Convert cursor to list to avoid cursor timeout if processing takes long, 
        # though here we just print. 
        # Also, we need to iterate twice (print and update)? 
        # No, the original code iterates to print count, then asks for confirmation.
        # But `documents_to_update` is a cursor. Iterating it once exhausts it.
        # So the original code had a bug: if it iterated to print, `count` would be correct, 
        # but then `documents_to_update` would be empty if we tried to use it again (though update_many uses a query, so it's fine).
        
        # Let's fix the logic:
        # 1. Count using count_documents
        # 2. Print some examples if needed (optional, or find a few)
        
        count = col.count_documents({"time_update": {"$gt": timestamp_31_oct_2024}})
        
        # If we want to print them as before:
        cursor = col.find({"time_update": {"$gt": timestamp_31_oct_2024}})
        for doc in cursor:
             print(f"Tìm thấy: {doc['ticker']} - time_update: {doc['time_update']} ({datetime.fromtimestamp(doc['time_update'])})")

        print(f"\nTổng số document cần update: {count}")

        if count > 0:
            confirm = input(f"\nBạn có muốn update {count} documents về ngày 31-10-2024? (y/n): ")
            if confirm.lower() == 'y':
                result = col.update_many(
                    {"time_update": {"$gt": timestamp_31_oct_2024}},
                    {"$set": {"time_update": timestamp_31_oct_2024}}
                )
                print(f"Đã update {result.modified_count} documents")
            else:
                print("Hủy bỏ")
        else:
            print("Không có document nào cần update")

    except Exception as e:
        print(f"Failed to connect to MongoDB or Error: {e}")
        exit(1)
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    main()
print("Finish")