# FILE: inference_cpu.py
import treelite_runtime
import numpy as np
import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# --- CẤU HÌNH ---
# [QUAN TRỌNG] Đổi đuôi file thành .so
MODEL_FILE = "./model/rf_model_cpu.so" 

API_BASE = "http://127.0.0.1:8000"
WINDOW_SIZE = 7
FEATURES = ['o', 'h', 'l', 'c', 'v']

# --- GIỮ NGUYÊN CÁC HÀM XỬ LÝ DATA (Copy từ bài trước) ---
# Hàm get_historical_data (đã fix mili-giây)
def get_historical_data(ticker, target_timestamp_ms):
    try:
        ts_seconds = target_timestamp_ms / 1000
        target_dt = datetime.fromtimestamp(ts_seconds)
        start_dt = target_dt - timedelta(days=30)
        from_ts_ms = int(start_dt.timestamp() * 1000)
        
        url = f"{API_BASE}/stock/ohlc"
        params = {
            "ticker": ticker,
            "from_timestamp": from_ts_ms, 
            "to_timestamp": target_timestamp_ms,
            "limit": 10000,
            "page": 1
        }
        res = requests.get(url, params=params, timeout=10)
        if res.status_code != 200: return None
        docs = res.json().get("documents", [])
        if not docs: return None
        return pd.DataFrame(docs)
    except: return None

# Hàm prepare_input_vector
def prepare_input_vector(df):
    if df.empty: return None
    df['date'] = pd.to_datetime(df['t'], unit='ms')
    df = df.set_index('date').sort_index()
    df = df[~df.index.duplicated(keep='first')]
    daily_df = df.resample('1D').agg({'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'}).ffill()
    df_ret = daily_df[FEATURES].pct_change().replace([np.inf, -np.inf], 0).dropna()
    if len(df_ret) < WINDOW_SIZE: return None
    return df_ret.tail(WINDOW_SIZE).values.flatten().astype(np.float32)

def predict_stock_cpu(ticker, timestamp_ms):
    if not os.path.exists(MODEL_FILE):
        print(f"Lỗi: Không tìm thấy file {MODEL_FILE}")
        return

    # 1. Load Model (Đã biên dịch)
    print(f"--- Đang load model CPU: {MODEL_FILE} ---")
    
    # Treelite Runtime sẽ load trực tiếp file .so
    predictor = treelite_runtime.Predictor(MODEL_FILE, verbose=False)

    print(f"--- Đang lấy dữ liệu {ticker} ---")
    df = get_historical_data(ticker, timestamp_ms)
    
    if df is None: return
    
    vec = prepare_input_vector(df)
    if vec is None: return

    # 2. Predict
    # Input shape: (1, 35)
    batch_input = np.expand_dims(vec, axis=0)
    
    try:
        pred = predictor.predict(batch_input)
        val = float(pred[0])
        
        date_str = datetime.fromtimestamp(timestamp_ms/1000).strftime('%Y-%m-%d %H:%M')
        print("="*40)
        print(f"DỰ BÁO CPU (Treelite) - {ticker}")
        print(f"Thời gian: {date_str}")
        print(f"Kết quả : {val:.4f} ({val*100:.2f}%)")
        print("="*40)
        
        if val > 0: print("-> Xu hướng: TĂNG")
        elif val < 0: print("-> Xu hướng: GIẢM")
        
    except Exception as e:
        print(f"Lỗi inference: {e}")

if __name__ == "__main__":
    predict_stock_cpu("FEX", 1759258800000)