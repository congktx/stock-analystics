from fastapi import APIRouter, HTTPException

from database.mongodb import MongoDB

from typing import Union

mongodb = MongoDB()

stock_router = APIRouter(
    prefix='/stock',
    tags=['stock']
)

@stock_router.get("/company-infos")
async def get_company_infos(ticker: Union[str | None] = None, exchange: Union[str | None] = None, from_timestamp: Union[int, None] = None, to_timestamp: Union[int, None] = None , limit: Union[int | None] = None, page: Union[int | None] = None):
    try:
        if limit == None:
            limit = 1000
        
        if page == None: 
            page = 1
        
        return mongodb.get_company_infos(ticker, exchange, from_timestamp, to_timestamp, limit, page)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@stock_router.get('/market-status')
async def get_market_status():
    try:
        return mongodb.get_market_status()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@stock_router.get('/ohlc')
async def get_ohlc(ticker: Union[str | None] = None, from_timestamp: Union[int | None] = None, to_timestamp: Union[int | None] = None, limit: Union[int | None] = None, page: Union[int | None] = None):
    try:
        if limit == None:
            limit = 1000
        
        if page == None: 
            page = 1

        return mongodb.get_ohlc(ticker, from_timestamp, to_timestamp, limit, page)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@stock_router.get('/news_sentiment')
async def get_news_sentiment(ticker: Union[str | None] = None, from_date: Union[str | None] = None, to_date: Union[str | None] = None, limit: Union[int | None] = None, page: Union[int | None] = None):
    try:
        if limit == None:
            limit = 1000
        
        if page == None: 
            page = 1
        
        return mongodb.get_news_sentiment(ticker, from_date, to_date, limit, page)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))