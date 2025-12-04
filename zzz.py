import mongoConnection as mongo

ohlc3s = mongo.db_na['ohlc3'].find()

for ohlc3 in ohlc3s:
  ohlc_find = mongo.db_na['ohlc'].find_one({'_id': ohlc3['_id']})
  if ohlc_find:
    print(ohlc3)
    mongo.db_na['ohlc3'].delete_one({'_id': ohlc3['_id']})