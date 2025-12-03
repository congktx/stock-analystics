from fastapi import FastAPI

from router import stock_router

app = FastAPI()

@app.get('/')
def read_root():
    return {"message": "Server is running"}

app.include_router(stock_router)