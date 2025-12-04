import json
import mongoConnection as mongo
import asyncio

async def addCompany():
  print(111)
  with open('stock-analystics.company_infos.json', 'r', encoding='utf-8') as file:
    companies = json.load(file)
    for company in companies:
      if company['_id'] == "XZO_1733011200.0":
        print(company)
      await mongo.company_infos_coll.find_one_and_update(
        {'_id': company['_id']},
        {'$set': company},
        upsert=True
      )

async def main():
  await addCompany()

if __name__ == "__main__":
  asyncio.run(main())
