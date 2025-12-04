import asyncio
import aiohttp
import mongoConnection as mongo
import json
import config
import datetime

async def fetch_ohlc_for_tickers():
  with open('split_ticker.json', 'r') as file:
    companies = json.load(file)

  async with aiohttp.ClientSession() as session:
    for ticker in companies["Cong"]:
            company_find = await mongo.company_infos_coll.find_one({"ticker": ticker})
            ohlc = await mongo.OHLC_coll.find_one(
              {"ticker": ticker},
              sort=[("t", -1)]
            )
            if ohlc and company_find and company_find.get('isQueryForOHLCByBTC3'):
              dt = datetime.datetime.fromtimestamp(int(ohlc["t"])/1000)
              day   = dt.day
              month = dt.month
              year  = dt.year
              # if (year > 2025 or month > 11 or day > 3): continue
              if (year > 2025 or month > 11 or day > 20): continue

            print(ticker)
            # time_from = '2024-01-01'
            time_from = '2025-11-03'
            # time_to = '2025-11-03'
            time_to = '2025-11-22'
            next_url = (
                f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/hour/'
                f'{time_from}/{time_to}/?adjusted=true&sort=asc&limit=50000&apiKey={config.POLYGONIO_TOKEN}'
            )
            if company_find and company_find.get('isQueryForOHLCByBTC3'):
              if company_find.get('next_url') and len(company_find['next_url'])>0 and ('cursor' in company_find['next_url']):
                  next_url = company_find['next_url']

            while True:
                try:
                  await asyncio.sleep(11)  
                  print(next_url)
                  if company_find:
                    await mongo.company_infos_coll.find_one_and_update(
                        {"_id": company_find["_id"]},
                        {"$set": {'next_url': next_url}}
                    )

                  async with session.get(next_url) as resp:
                      data = await resp.json()

                  if not data.get('results'):
                      break

                  results = data['results']
                  print(len(results))
                  if len(results) == 0:
                      break

                  for ohlc in results:
                      try:
                          document = {
                              "_id": f"{ticker}_{ohlc['t']}",
                              "ticker": ticker,
                              "t": ohlc['t'],
                              "o": ohlc['o'],
                              "h": ohlc['h'],
                              "l": ohlc['l'],
                              "c": ohlc['c'],
                              "v": ohlc['v'],
                          }
                          await mongo.OHLC_coll.find_one_and_update(
                              {"_id": document["_id"]},
                              {"$set": document},
                              upsert=True
                          )
                      except Exception as e:
                          print(e)

                  if not data.get('next_url'):
                      break

                  next_url = data['next_url'] + f'&apiKey={config.POLYGONIO_TOKEN}'
                except Exception as e:
                    print(e)

            if company_find:
                await mongo.company_infos_coll.find_one_and_update(
                    {"_id": company_find["_id"]},
                    {"$set": {'isQueryForOHLCByBTC3': True}}
                )

async def main():
    await fetch_ohlc_for_tickers()

if __name__ == "__main__":
    asyncio.run(main())