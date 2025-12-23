from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017")
db = client["stock-analystics"]
col = db["company_infos"]

timestamp_31_oct_2024 = int(datetime(2024, 10, 31, 0, 0, 0).timestamp())

print(f"Timestamp ngày 31-10-2024: {timestamp_31_oct_2024}")

documents_to_update = col.find({"time_update": {"$gt": timestamp_31_oct_2024}})

count = 0
for doc in documents_to_update:
    print(f"Tìm thấy: {doc['ticker']} - time_update: {doc['time_update']} ({datetime.fromtimestamp(doc['time_update'])})")
    count += 1

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

print("Finish")