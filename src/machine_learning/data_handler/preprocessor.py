from asyncio.log import logger
import os
import pandas as pd
from pathlib import Path
from machine_learning.enums import FutureDays
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import time

class Preprocessor:
    def __init__(self):
        input_path = os.getcwd() + "/machine_learning/data_handler"
        self.input_path = input_path
        self.ohlc_path = input_path + "/raw_data/ohlc.csv"
        self.list_tickers = input_path + "/raw_data/list_tickers.csv"
        self.output_dir = Path(input_path) / "preprocessed_data"
        self.future_days = FutureDays.WEEK.value
        self.tickers = pd.read_csv(self.list_tickers, usecols=['ticker']).ticker.str.upper().unique()
        self.processed_tickers = pd.read_csv(input_path + "/preprocessed_data/processed_tickers.csv", usecols=['ticker'])
        self.feature_cols = [
            'ticker_id', 'ticker', 'news_id', 'date', 'overall_sentiment_score', "relevance_score", 
            "ticker_sentiment_score", 'close_price', 'future_avg_close', 'label'
        ]
        self.news_sentiment_chunks = None
        self.news_ticker_chunks = None
        self.tickers_with_index_chunks = None
        
    def process_ticker(self, ticker):
        """Xử lý từng ticker để tránh load toàn bộ CSV"""
        #measure time
        t0 = time.perf_counter()
        
        if not isinstance(ticker, str):
            logger.warning(f"[WARN] Invalid ticker value: {ticker}")
            return
        ticker_upper = ticker.upper()
        print(f"[INFO] Processing ticker: {ticker_upper}")
        
        # 2️⃣ Load OHLC của ticker theo chunk
        ohlc_cols = ['ticker','t','c']
        ohlc_iter = pd.read_csv(self.ohlc_path, usecols=ohlc_cols, chunksize=10**6)
        ohlc_list = []
        
        t2 = time.perf_counter()
        for chunk in ohlc_iter:
            chunk = chunk[chunk['ticker'].str.upper() == ticker_upper].copy()
            if not chunk.empty:
                chunk['date'] = pd.to_datetime(chunk['t'], unit='ms')
                chunk['date'] = chunk['date'].dt.floor('D')  # giữ datetime64[ns] nhưng bỏ giờ
                chunk = chunk.groupby('date')['c'].last().reset_index()  # close cuối ngày
                ohlc_list.append(chunk)
        
        # #try to process all data at a time
        # ohlc_df = pd.read_csv(self.ohlc_path, usecols=ohlc_cols)
        # ohlc_df = ohlc_df[ohlc_df['ticker'].str.upper() == ticker_upper].copy()
        # if not ohlc_df.empty:
        #     ohlc_df['date'] = pd.to_datetime(ohlc_df['t'], unit='ms')
        #     ohlc_df['date'] = ohlc_df['date'].dt.floor('D')  # giữ datetime64[ns] nhưng bỏ giờ
        #     ohlc_df = ohlc_df.groupby('date')['c'].last().reset_index()  # close cuối ngày
        #     ohlc_list.append(ohlc_df)
        if not ohlc_list:
            print(f"[WARN] No OHLC data for {ticker_upper}")
            self.mark_ticker_processed(ticker_upper)
            return
        
        print(time.perf_counter() - t2)
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
        
        #process sentiment data
        
        if self.news_sentiment_chunks is None:
            self.news_sentiment_chunks = pd.read_csv(
                self.input_path + "/raw_data/news_sentiment_processed.csv",
                parse_dates=['time_published'],
                usecols=['id', 'time_published', 'overall_sentiment_score'],
                # chunksize=10**6
            ).rename(columns={'id': 'news_id', 'time_published': 'date'})
        
        if self.news_ticker_chunks is None:
            self.news_ticker_chunks = pd.read_csv(
                self.input_path + "/raw_data/news_ticker_mapping.csv",
                usecols=["news_id", "ticker", "relevance_score", "ticker_sentiment_score"],
                # chunksize=10**6
            )
        
        if self.tickers_with_index_chunks is None:
            self.tickers_with_index_chunks = pd.read_csv(
                self.input_path + "/raw_data/list_tickers.csv",
                # chunksize=10**6
            )
        
        t1 = time.perf_counter()
        df = self.news_sentiment_chunks.merge(
            self.news_ticker_chunks,
            on='news_id',
            how='inner'
        )
        df = df.merge(
            self.tickers_with_index_chunks,
            on='ticker',
            how='inner'
        )
        df = df.merge(
            ohlc_df,
            on=['date'],
            how='inner'
        )
        print(time.perf_counter() - t1)

        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(ohlc_df)
        df.rename(columns={'c':'close_price'}, inplace=True)
        df.dropna(subset=['close_price'], inplace=True)
        df.dropna(subset=['future_avg_close'], inplace=True)
        
        # 5️⃣ Lựa chọn nhãn: tăng/giảm nhẹ/mạnh dựa trên tỷ lệ thay đổi
        df['label'] = (df['future_avg_close'] - df['close_price']) / df['close_price']
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(df)
        # phân loại nhãn
        bins = [-float('inf'), -0.05, -0.02, 0.02, 0.05, float('inf')]
        labels = ['strong_down', 'down', 'stable', 'up', 'strong_up']
        df['label'] = pd.cut(df['label'], bins=bins, labels=labels)
        
        df = df[self.feature_cols]
        if df.empty:
            print(f"[WARN] No data after processing for {ticker_upper}")
            self.mark_ticker_processed(ticker_upper)
            return
        #Lưu kết quả theo ticker
        data_file = self.output_dir / f"data.csv"
        write_header = not data_file.exists() or os.stat(data_file).st_size == 0
        df.to_csv(data_file, mode='a', index=False, header=write_header)
        self.mark_ticker_processed(ticker_upper)
        
        print(f"[INFO] Saved {ticker_upper} -> {data_file}")
        print(time.perf_counter() - t0)
    
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
            print(len(self.tickers))
            if ticker in self.processed_tickers['ticker'].values:
                print(f"[INFO] Ticker {ticker} already processed. Skipping.")
                continue 
            self.process_ticker(ticker)
        print("[INFO] Preprocessing completed!")
        
    def plot_preprocessed_data(self, data_file, plotted_columns):
        """make statistics on the preprocessed data"""
        #read file
        data_file = self.output_dir / data_file
        if not plotted_columns:
            plotted_columns = ['ticker_id','overall_sentiment_score','relevance_score','ticker_sentiment_score','close_price','label']
        df_iter = pd.read_csv(data_file, usecols=plotted_columns)
        #plot distribution of all features
        for col in plotted_columns:
            plt.figure(figsize=(10, 6))
            sns.histplot(df_iter[col].dropna(), bins=50)
            plt.title(f'Distribution of {col}')
            plt.xlabel(col)
            plt.ylabel('Frequency')
            plt.grid(True)
            plt.show()
            
    def create_test_file(self, from_date, to_date=datetime.today().strftime("%y-%b-%d")):
        """Create a test file from preprocessed data within a date range"""
        data_file = self.output_dir / "data.csv"
        test_file = self.output_dir / "test_data.csv"
        if test_file.exists():
            test_file.unlink()
            print(f"[INFO] Removed existing file: {test_file}")
        df_iter = pd.read_csv(
            data_file,
            parse_dates=['date'],
            chunksize=10**6
        )
        
        first_write = True
        for chunk in df_iter:
            mask = (chunk['date'] >= from_date) & (chunk['date'] <= to_date)
            test_chunk = chunk.loc[mask]

            if not test_chunk.empty:
                test_chunk.to_csv(
                    test_file,
                    mode='a',
                    index=False,
                    header=first_write
                )
                first_write = False
            print(f"[INFO] Test file created in chunk: {test_file}")
            
    def index_tickers(self, file_location):
        file_location = self.input_path + f"/{file_location}"
        #load tickers from json file
        df = pd.read_json(file_location)
        tickers = df["companies"].dropna().str.upper().tolist()
        tickers = list(dict.fromkeys(tickers))
        ticker_df = pd.DataFrame({
            "ticker_id": range(1, len(tickers) + 1),
            "ticker": tickers
        })
        ticker_df.to_csv(self.input_path + "/preprocessed_data/list_tickers.csv", index=False)
        
    def change_labels(self):
        """Change labels according to the provided mapping dictionary"""
        data_file = self.output_dir / f"data.csv"
        dest_file = self.output_dir / f"data_changed_label.csv"
        df_iter = pd.read_csv(data_file, chunksize=10**6)
        #data có trường future_avg_close và trường close_price để tính lại label
        for chunk in df_iter:
            chunk['label'] = (chunk['future_avg_close'] - chunk['close_price']) / chunk['close_price']
            print(chunk['label'].head(1))
            bins = [-float('inf'), -0.023, -0.0037, 0.0037, 0.023, float('inf')]
            labels = ['strong_down', 'down', 'stable', 'up', 'strong_up']
            chunk['label'] = pd.cut(chunk['label'], bins=bins, labels=labels)
            print(chunk['label'].head(1))
            chunk.to_csv(
                dest_file,
                mode='a',
                index=False,
                header=not dest_file.exists() or os.stat(dest_file).st_size == 0
            )
            print(f"[INFO] Changed labels and saved to {dest_file}")
            
    def eda(self, data_file):
        """Perform EDA on the preprocessed data"""
        data_file = self.output_dir / data_file
        
        feature_cols = ['ticker_id','overall_sentiment_score','relevance_score','ticker_sentiment_score','close_price','label']
        df = pd.read_csv(data_file, usecols=feature_cols)
        
        print("\n=== Feature statistics ===")
        print(df[feature_cols].describe().T)
        print("\n=== Label distribution ===")
        print(df["label"].value_counts(normalize=True))
        
        # ===== 3. Phân bố từng feature theo label =====
        for col in feature_cols:
            plt.figure(figsize=(6, 4))
            try:
                sns.kdeplot(
                data=df,
                x=col,
                hue="label",
                common_norm=False,
                fill=True,
                alpha=0.4
            )
                plt.title(f"Distribution of {col} by label")
                plt.tight_layout()
                plt.show()
            except ():
                continue
            
    def remark_label(self, labels=['down', 'stable', 'up']):
        """Change labels according to the provided mapping dictionary"""
        data_file = self.output_dir / f"data_changed_label.csv"
        dest_file = self.output_dir / f"data_changed_label_v2.csv"
        df_iter = pd.read_csv(data_file, chunksize=10**6)
        #data có trường future_avg_close và trường close_price để tính lại label
        for chunk in df_iter:
            chunk['label'] = (chunk['future_avg_close'] - chunk['close_price']) / chunk['close_price']
            print(chunk['label'].head(1))
            bins = [-float('inf'), -0.01, 0.01, float('inf')]
            chunk['label'] = pd.cut(chunk['label'], bins=bins, labels=labels)
            chunk.to_csv(
                dest_file,
                mode='a',
                index=False,
                header=not dest_file.exists() or os.stat(dest_file).st_size == 0
            )
            print(f"[INFO] Changed labels and saved to {dest_file}")
        
if __name__ == "__main__":
    preprocessor = Preprocessor()
    # preprocessor.plot_preprocessed_data(data_file="data_changed_label.csv", plotted_columns=["label"])
    # preprocessor.remark_label()
    # preprocessor.plot_preprocessed_data(data_file="data_changed_label_v2.csv", plotted_columns=["label"])
    # preprocessor.create_test_file(from_date="2025-01-01")
    preprocessor.eda(data_file="data_changed_label.csv")