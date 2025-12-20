from asyncio.log import logger
import os
import pandas as pd
from pathlib import Path
from machine_learning.enums import FutureDays
import numpy as np


class Preprocessor:
    def __init__(self):
        input_path = os.getcwd() + "/machine_learning/data_handler"
        self.ohlc_path = input_path + "/raw_data/ohlc.csv"
        self.sentiment_path = input_path + "/raw_data/ticker_sentiment_daily.csv"
        self.output_dir = Path(input_path) / "preprocessed_data"
        self.future_days = FutureDays.WEEK.value
        self.tickers = pd.read_csv(self.sentiment_path, usecols=['ticker']).ticker.str.upper().unique()
        self.processed_tickers = pd.read_csv(input_path + "/preprocessed_data/processed_tickers.csv", usecols=['ticker'])
        
    def process_ticker(self, ticker):
        """Xử lý từng ticker để tránh load toàn bộ CSV"""
        if not isinstance(ticker, str):
            logger.warning(f"[WARN] Invalid ticker value: {ticker}")
            return
        ticker_upper = ticker.upper()
        print(f"[INFO] Processing ticker: {ticker_upper}")
        
        # 1️⃣ Load sentiment của ticker
        sent = pd.read_csv(self.sentiment_path, parse_dates=['date'])
        sent = sent[sent['ticker'].str.upper() == ticker_upper].copy()
        sent.sort_values('date', inplace=True)
        
        if sent.empty:
            logger.warning(f"[WARN] No sentiment data for {ticker_upper}")
            self.mark_ticker_processed(ticker_upper)
            return
        
        # 2️⃣ Load OHLC của ticker theo chunk
        ohlc_cols = ['ticker','t','c']
        ohlc_iter = pd.read_csv(self.ohlc_path, usecols=ohlc_cols, chunksize=10**6)
        ohlc_list = []
        
        for chunk in ohlc_iter:
            chunk = chunk[chunk['ticker'].str.upper() == ticker_upper].copy()
            if not chunk.empty:
                chunk['date'] = pd.to_datetime(chunk['t'], unit='ms')
                chunk['date'] = chunk['date'].dt.floor('D')  # giữ datetime64[ns] nhưng bỏ giờ
                chunk = chunk.groupby('date')['c'].last().reset_index()  # close cuối ngày
                ohlc_list.append(chunk)
        
        if not ohlc_list:
            print(f"[WARN] No OHLC data for {ticker_upper}")
            self.mark_ticker_processed(ticker_upper)
            return
        
        ohlc_df = pd.concat(ohlc_list).sort_values('date')
        # print(ohlc_df["date"].info())
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(ohlc_df)
        # Tạo dictionary để tra cứu giá theo ngày
        price_dict = dict(zip(ohlc_df["date"], ohlc_df["c"]))

        future_avg = []

        for current_date in ohlc_df["date"]:
            # các ngày tiếp theo
            next_dates = [current_date + pd.Timedelta(days=d) for d in range(1, 8)]
            
            # lấy những ngày có giá
            available_prices = [price_dict[d] for d in next_dates if d in price_dict]
            
            # nếu có ít nhất 4 ngày có giá, tính trung bình, nếu không => NaN
            if len(available_prices) >= 4:
                avg = np.mean(available_prices)
            else:
                avg = np.nan
            
            future_avg.append(avg)

        ohlc_df["future_avg_close"] = future_avg

        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(ohlc_df)
        # 3️⃣ Join sentiment với giá close cùng ngày
        df = sent.merge(ohlc_df, on='date', how='left')
        
        df.rename(columns={'c':'close_price'}, inplace=True)
        df.dropna(subset=['close_price'], inplace=True)
        
        # 4️⃣ Tính nhãn: future_avg_close N ngày
        df.dropna(subset=['future_avg_close'], inplace=True)
        
        # 5️⃣ Lựa chọn nhãn: tăng/giảm nhẹ/mạnh dựa trên tỷ lệ thay đổi
        df['label'] = (df['future_avg_close'] - df['close_price']) / df['close_price']
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(df)
        # phân loại nhãn
        bins = [-float('inf'), -0.05, -0.02, 0.02, 0.05, float('inf')]
        labels = ['strong_down', 'down', 'stable', 'up', 'strong_up']
        df['label'] = pd.cut(df['label'], bins=bins, labels=labels)
        
        # 6️⃣ Chọn cột đầu vào
        feature_cols = [
            'ticker', 'date', 'news_count', 'avg_sentiment_score', 
            'bullish_count','bearish_count','neutral_count',
            'top_sentiment_score', 'close_price', 'future_avg_close', 'label'
        ]
        df = df[feature_cols]
        if df.empty:
            print(f"[WARN] No data after processing for {ticker_upper}")
            self.mark_ticker_processed(ticker_upper)
            return
        # 7️⃣ Lưu kết quả parquet theo ticker
        data_file = self.output_dir / f"data.csv"
        write_header = not data_file.exists() or os.stat(data_file).st_size == 0
        df.to_csv(data_file, mode='a', index=False, header=write_header)
        self.mark_ticker_processed(ticker_upper)
        
        print(f"[INFO] Saved {ticker_upper} -> {data_file}")
    
    def mark_ticker_processed(self, ticker_upper):
        process_tickers_file = self.output_dir / "processed_tickers.csv"
        write_header = (
            not process_tickers_file.exists() or 
            os.stat(process_tickers_file).st_size == 0
        )
        pd.DataFrame([ticker_upper], columns=['ticker']).to_csv(
            process_tickers_file,
            mode='a',
            index=False,
            header=write_header
        )
        
    def run(self):
        for ticker in self.tickers:
            if ticker in self.processed_tickers['ticker'].values:
                print(f"[INFO] Ticker {ticker} already processed. Skipping.")
                continue 
            self.process_ticker(ticker)
        print("[INFO] Preprocessing completed!")