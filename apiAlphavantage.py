import asyncio
import aiohttp
import json
import config
import mongoConnection as mongo  
import turn_vpn 

async def fetch_sentiment():
  vpn_enable = True

  while True:
    with open('split_ticker.json', 'r') as file:
        companies = json.load(file)

    cnt = 0
    async with aiohttp.ClientSession() as session:
        if vpn_enable: 
          await turn_vpn.turn_off_vpn()
          vpn_enable = False
        else: 
          await turn_vpn.turn_on_vpn()
          vpn_enable = True
        for ticker in companies["Cong"]:
            try:
                company_find = await mongo.company_infos_coll.find_one({"ticker": ticker})
                if company_find and company_find.get('isQueryForNewSentimentByBTC2'):
                    continue

                print(ticker)

                time_from = '20251105T0000'
                time_to = '20251122T0000'
                url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={config.ALPHAVANTAGE_TOKEN}&time_from={time_from}&time_to={time_to}'
                print(url)

                async with session.get(url) as resp:
                    data = await resp.json()
                print(data)

                if (data.get('Note')):
                  if (data['Note'] == 'We have detected your API key as OGRH8YTFZEOEOLWF and our standard API rate limit is 25 requests per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a higher API call frequency.'):
                    break

                if (data.get('Information')):
                  if (data['Information'] == "Invalid inputs. Please refer to the API documentation https://www.alphavantage.co/documentation#newsapi and try again."):
                    if company_find:
                      await mongo.company_infos_coll.find_one_and_update(
                          {"_id": company_find['_id']},
                          {"$set": {'isQueryForNewSentimentByBTC2': True}}
                      )  
                  if (data['Information'] == 'We have detected your API key as OGRH8YTFZEOEOLWF and our standard API rate limit is 25 requests per day. Please subscribe to any of the premium plans at https://www.alphavantage.co/premium/ to instantly remove all daily rate limits.'):
                    break

                if not data.get('feed'):
                  if company_find:
                    await mongo.company_infos_coll.find_one_and_update(
                        {"_id": company_find['_id']},
                        {"$set": {'isQueryForNewSentimentByBTC2': True}}
                    )  
                  print(f"{ticker}: no data feed")
                  continue

                feed = data['feed']
                for fee in feed:
                    try:
                        fee['_id'] = fee['title']
                        await mongo.news_sentiment_coll.find_one_and_update(
                          {"_id": fee['_id']},
                          {"$set": fee},
                          upsert=True
                        )
                    except Exception as e:
                        print(f"Mongo error: {e}")

                cnt += 1
                print(f"{cnt}: done {ticker}")

                if company_find:
                    await mongo.company_infos_coll.find_one_and_update(
                        {"_id": company_find['_id']},
                        {"$set": {'isQueryForNewSentimentByBTC2': True}}
                    )
                    
                await asyncio.sleep(5)

            except Exception as e:
                print(f"Error with {ticker}: {e}")

async def main():
    await fetch_sentiment()

if __name__ == "__main__":
    asyncio.run(main())
