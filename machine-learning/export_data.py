# FILE: 1_export_data.py
import requests
import pandas as pd
import numpy as np
import os
import csv

# --- CẤU HÌNH ---
API_BASE = "http://127.0.0.1:8000"
WINDOW_SIZE = 7
OUTPUT_FILE = "training_data_returns.csv" # Đổi tên file để tránh nhầm lẫn

FEATURES = ['o', 'h', 'l', 'c', 'v']

def get_all_tickers():
    print("--- Đang lấy danh sách Ticker ---")
    tickers = set()
    page = 1
    while True:
        try:
            url = f"{API_BASE}/stock/company-infos?page={page}"
            res = requests.get(url, timeout=10)
            if res.status_code != 200: break
            data = res.json()
            docs = data.get("documents", [])
            if not docs: break
            for doc in docs:
                if 'ticker' in doc: tickers.add(doc['ticker'])
            if data.get("page_id") >= data.get("page_count"): break
            page += 1
        except Exception as e:
            print(f"Lỗi lấy ticker: {e}")
            break
    print(f"-> Tìm thấy {len(tickers)} ticker.")
    return list(tickers)

def fetch_ohlc_data(ticker):
    all_docs = []
    page = 1
    while True:
        try:
            url = f"{API_BASE}/stock/ohlc?ticker={ticker}&page={page}"
            res = requests.get(url, timeout=5)
            if res.status_code != 200: break
            data = res.json()
            docs = data.get("documents", [])
            if not docs: break
            all_docs.extend(docs)
            if data.get("page_id") >= data.get("page_count"): break
            page += 1
        except: break
    return pd.DataFrame(all_docs)

def process_ticker_data(df):
    if df.empty: return []
    
    # 1. Resample
    df['date'] = pd.to_datetime(df['t'], unit='ms')
    df = df.set_index('date').sort_index()
    df = df[~df.index.duplicated(keep='first')]
    
    # Dùng ffill để lấp đầy ngày nghỉ (giá đi ngang)
    daily_df = df.resample('1D').agg({
        'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'
    }).ffill()
    
    # Lọc bỏ mã rác (giá quá nhỏ)
    if (daily_df['c'] <= 0.01).sum() > 0:
        return []

    # --- [KEY CHANGE] CHUYỂN SANG % RETURNS ---
    # pct_change(): Tính % thay đổi so với dòng trước đó
    # Ví dụ: Giá 100 -> 101 thì pct_change là 0.01
    df_ret = daily_df[FEATURES].pct_change()
    
    # Bỏ dòng đầu tiên (NaN)
    df_ret = df_ret.dropna()
    
    # Xử lý vô cực (nếu có chia cho 0)
    df_ret = df_ret.replace([np.inf, -np.inf], 0)
    
    if len(df_ret) <= WINDOW_SIZE + 1: return []

    processed_rows = []
    vals = df_ret.values # Dữ liệu giờ là số thập phân nhỏ (0.01, -0.02...)
    
    # 2. Tạo Sliding Window
    for i in range(len(vals) - WINDOW_SIZE):
        input_features = vals[i : i + WINDOW_SIZE].flatten().tolist()
        
        # Target: % Tăng trưởng giá OPEN của ngày tiếp theo
        target_return = vals[i + WINDOW_SIZE][0] 
        
        # [QUAN TRỌNG] Lọc nhiễu dữ liệu (Data Cleaning)
        # Nếu tăng > 200% (2.0) hoặc giảm > 90% (-0.9) trong 1 ngày -> Thường là lỗi data hoặc chia tách cổ phiếu
        if target_return > 2.0 or target_return < -0.9:
            continue
            
        row = input_features + [target_return]
        processed_rows.append(row)
            
    return processed_rows

def main():
    header = []
    for i in range(WINDOW_SIZE):
        for f in FEATURES:
            header.append(f"day{i}_{f}_ret")
    header.append("target_open_return")
    
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)

    tickers = get_all_tickers()
    
    print(f"--- Bắt đầu xử lý (Returns Mode) -> {OUTPUT_FILE} ---")
    total_samples = 0
    
    with open(OUTPUT_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        for idx, ticker in enumerate(tickers):
            df_raw = fetch_ohlc_data(ticker)
            rows = process_ticker_data(df_raw)
            if rows:
                writer.writerows(rows)
                total_samples += len(rows)
            
            if (idx + 1) % 10 == 0:
                print(f"Đã xử lý {idx + 1}/{len(tickers)}. Mẫu: {total_samples}")

    print(f"Xong! File: {os.path.abspath(OUTPUT_FILE)}")

if __name__ == "__main__":
    main()