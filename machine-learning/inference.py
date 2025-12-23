# FILE: inference_pytorch.py
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import requests
import joblib
import os
from datetime import datetime, timedelta, timezone
import csv

import warnings
from sklearn.exceptions import InconsistentVersionWarning
warnings.filterwarnings("ignore", category=InconsistentVersionWarning)

# --- CẤU HÌNH ---
MODEL_FILE = "./model/best_pytorch_model.pth"
SCALER_FILE = "./model/scaler.pkl"
API_BASE = "http://127.0.0.1:8000"
WINDOW_SIZE = 7
FEATURES = ['o', 'h', 'l', 'c', 'v']

# =========================================================
# 1. ĐỊNH NGHĨA KIẾN TRÚC MODEL
# =========================================================

class StockPredictor(nn.Module):
    def __init__(self, input_dim):
        super(StockPredictor, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )

    def forward(self, x):
        return self.net(x)


def get_historical_data(ticker, target_timestamp_ms):
    """
    Lấy dữ liệu QUÁ KHỨ dựa trên page_count từ API
    """
    try:
        # 1. Tính toán khoảng thời gian (30 ngày)
        ts_seconds = target_timestamp_ms / 1000
        target_dt = datetime.fromtimestamp(ts_seconds)
        start_dt = target_dt - timedelta(days=30)
        from_ts_ms = int(start_dt.timestamp() * 1000)

        url = f"{API_BASE}/stock/ohlc"

        # Tham số cơ bản (chưa có page)
        base_params = {
            "ticker": ticker,
            "from_timestamp": from_ts_ms,
            "to_timestamp": target_timestamp_ms,
            # Số lượng record mỗi trang (tùy chỉnh theo API server của bạn)
            "limit": 1000
        }

        all_docs = []

        # --- BƯỚC 1: Gọi Page 1 trước để lấy Meta Data (page_count) ---
        base_params["page"] = 1

        res = requests.get(url, params=base_params, timeout=10)
        if res.status_code != 200:
            print(f"❌ Lỗi API Page 1: {res.status_code}")
            return None

        data = res.json()

        # Lấy docs trang 1
        docs = data.get("documents", [])
        if not docs:
            return None
        all_docs.extend(docs)

        # Lấy tổng số trang
        page_count = data.get("page_count", 1)

        # --- BƯỚC 2: Vòng lặp lấy các trang còn lại (nếu có) ---
        if page_count > 1:
            # Dùng Session để tái sử dụng kết nối TCP -> Tải nhanh hơn
            with requests.Session() as session:
                for p in range(2, page_count + 1):
                    base_params["page"] = p
                    try:
                        # Gọi API các trang tiếp theo
                        r = session.get(url, params=base_params, timeout=10)

                        if r.status_code == 200:
                            page_data = r.json()
                            page_docs = page_data.get("documents", [])
                            all_docs.extend(page_docs)
                            # print(f"-> Đã tải xong page {p}/{page_count}")
                        else:
                            print(f"⚠️ Lỗi tại page {p}: {r.status_code}")

                    except Exception as e:
                        print(f"⚠️ Lỗi kết nối page {p}: {e}")
                        continue  # Bỏ qua trang lỗi, đi tiếp

        if not all_docs:
            return None

        # --- BƯỚC 3: Xử lý DataFrame ---
        df = pd.DataFrame(all_docs)

        # Xóa trùng lặp (đề phòng API trả trùng bản ghi ở ranh giới các trang)
        if 't' in df.columns:
            df = df.drop_duplicates(subset=['t'])
            df = df.sort_values(by='t')

        return df

    except Exception as e:
        print(f"❌ Lỗi get_historical_data: {e}")
        return None


def get_future_data(ticker, current_timestamp_ms):
    """[MỚI] Lấy dữ liệu TƯƠNG LAI (Ngày hôm sau) để kiểm chứng kết quả"""
    try:
        # Lấy từ thời điểm hiện tại + 1 giây đến 7 ngày sau (đề phòng cuối tuần/lễ)
        from_ts = current_timestamp_ms + 1000
        to_ts = current_timestamp_ms + (7 * 24 * 60 * 60 * 1000)

        url = f"{API_BASE}/stock/ohlc"
        params = {
            "ticker": ticker,
            "from_timestamp": from_ts,
            "to_timestamp": to_ts,
            "limit": 1,  # Chỉ cần lấy cây nến đầu tiên tiếp theo
            "page": 1
        }
        res = requests.get(url, params=params, timeout=5)
        if res.status_code != 200:
            return None
        docs = res.json().get("documents", [])
        if not docs:
            return None
        return docs[0]  # Trả về cây nến tiếp theo (dict)
    except:
        return None


def prepare_input_vector(df):
    if df.empty:
        return None, None

    # 1. Xử lý thời gian
    df['date'] = pd.to_datetime(df['t'], unit='ms')
    df = df.set_index('date').sort_index()
    df = df[~df.index.duplicated(keep='first')]

    # Lấy giá Close tại thời điểm dự báo để tính lợi nhuận thực tế sau này
    current_close_price = df.iloc[-1]['c']

    # 2. Resample 1D & Fill
    daily_df = df.resample('1D').agg({
        'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'
    }).ffill()

    # 3. Tính Returns
    df_ret = daily_df[FEATURES].pct_change().replace(
        [np.inf, -np.inf], 0).dropna()

    if len(df_ret) < WINDOW_SIZE:
        print(f"Thiếu dữ liệu: Cần {WINDOW_SIZE} ngày, có {len(df_ret)}")
        return None, None

    # 4. Flatten
    vec = df_ret.tail(WINDOW_SIZE).values.flatten().astype(np.float32)
    return vec, current_close_price

# =========================================================
# 3. HÀM DỰ ĐOÁN (INFERENCE)
# =========================================================


def predict_stock_pytorch(ticker, timestamp_ms):
    # --- Kiểm tra file ---
    if not os.path.exists(MODEL_FILE) or not os.path.exists(SCALER_FILE):
        print(f"❌ Lỗi: Thiếu file model hoặc scaler.")
        return

    # --- 1. Load Model & Scaler ---
    scaler = joblib.load(SCALER_FILE)
    device = torch.device('cpu')
    model = StockPredictor(input_dim=35)

    try:
        model.load_state_dict(torch.load(MODEL_FILE, map_location=device))
        model.to(device)
        model.eval()
    except Exception as e:
        print(f"❌ Lỗi load model: {e}")
        return

    # --- 2. Lấy dữ liệu Input ---
    print(f"--- Đang lấy dữ liệu {ticker} ---")
    df = get_historical_data(ticker, timestamp_ms)
    if df is None:
        return

    # prepare_input_vector trả về cả vector input và giá hiện tại
    raw_vec, current_price = prepare_input_vector(df)
    if raw_vec is None:
        return

    # --- 3. Dự báo (Model) ---
    pred_value = 0.0
    try:
        input_reshaped = raw_vec.reshape(1, -1)
        input_scaled = scaler.transform(input_reshaped)
        input_tensor = torch.tensor(
            input_scaled, dtype=torch.float32).to(device)

        with torch.no_grad():
            pred_tensor = model(input_tensor)
            pred_value = pred_tensor.item()
    except Exception as e:
        print(f"❌ Lỗi dự báo: {e}")
        return

    # --- 4. Kiểm tra Thực tế (Actual) ---
    actual_return = None
    next_candle = get_future_data(ticker, timestamp_ms)

    if next_candle:
        next_price = next_candle['c']
        # Công thức Return: (Giá sau - Giá trước) / Giá trước
        actual_return = (next_price - current_price) / current_price

        next_date = datetime.fromtimestamp(
            next_candle['t']/1000).strftime('%Y-%m-%d')
    else:
        next_date = "N/A"

    # --- 5. Hiển thị Kết quả ---
    date_str = datetime.fromtimestamp(
        timestamp_ms/1000).strftime('%Y-%m-%d %H:%M')

    print("\n" + "="*50)
    print(f"📊 KẾT QUẢ DỰ BÁO - MÃ: {ticker}")
    print(f"🕒 Thời gian Input: {date_str}")
    print(f"💲 Giá tham chiếu : {current_price}")
    print("-" * 50)

    # In Dự báo
    print(f"🤖 Model Dự báo   : {pred_value:.6f} ({pred_value*100:+.2f}%)")

    # In Thực tế (Nếu có)
    if actual_return is not None:
        print(
            f"📈 Thực tế (T+1)  : {actual_return:.6f} ({actual_return*100:+.2f}%)")
        print(f"📅 Ngày thực tế   : {next_date}")

        # Đánh giá sai số
        diff = abs(pred_value - actual_return)
        print(f"⚠️ Sai lệch (Abs) : {diff:.6f}")

        # Kiểm tra đúng chiều xu hướng?
        trend_pred = "TĂNG" if pred_value > 0 else "GIẢM"
        trend_real = "TĂNG" if actual_return > 0 else "GIẢM"

        if trend_pred == trend_real:
            print(f"✅ Bắt đúng xu hướng: {trend_real}")
        else:
            print(
                f"❌ Sai xu hướng (Dự báo {trend_pred} nhưng thực tế {trend_real})")
    else:
        print("❓ Thực tế        : Chưa có dữ liệu (Tương lai)")

    print("="*50)


def predict_next_price(ticker, timestamp_ms):
    """
    Hàm trả về GIÁ DỰ KIẾN (Predicted Price) thay vì % returns
    """
    # 1. Kiểm tra file
    if not os.path.exists(MODEL_FILE) or not os.path.exists(SCALER_FILE):
        print("❌ Thiếu model hoặc scaler")
        return None

    # 2. Load Model & Scaler
    try:
        scaler = joblib.load(SCALER_FILE)
        device = torch.device('cpu')
        model = StockPredictor(input_dim=35)
        model.load_state_dict(torch.load(MODEL_FILE, map_location=device))
        model.to(device)
        model.eval()
    except Exception as e:
        print(f"❌ Lỗi load model: {e}")
        return None

    # 3. Lấy dữ liệu
    df = get_historical_data(ticker, timestamp_ms)
    if df is None:
        # print(f"⚠️ Không có dữ liệu cho {ticker}")
        return None

    # 4. Chuẩn bị vector & Lấy giá hiện tại
    raw_vec, current_price = prepare_input_vector(df)
    if raw_vec is None:
        # print(f"⚠️ Không đủ dữ liệu 7 ngày cho {ticker}")
        return None

    # 5. Dự báo % biến động
    try:
        input_reshaped = raw_vec.reshape(1, -1)
        input_scaled = scaler.transform(input_reshaped)
        input_tensor = torch.tensor(
            input_scaled, dtype=torch.float32).to(device)

        with torch.no_grad():
            # Kết quả dạng % (VD: 0.02)
            pred_return = model(input_tensor).item()

        # 6. TÍNH GIÁ DỰ KIẾN
        # Công thức: Giá dự báo = Giá hiện tại * (1 + %Biến động)
        predicted_price = current_price * (1 + pred_return)

        return predicted_price

    except Exception as e:
        print(f"❌ Lỗi tính toán: {e}")
        return None


# =========================================================
# 4. MAIN - CHẠY THỬ NGHIỆM
# =========================================================
if __name__ == "__main__":
    ticker = "NVDA"  
    print(f"--- DỰ BÁO GIÁ CHO {ticker} (THÁNG 10/2025) ---\n")
    print(f"{'Ngày':<15} | {'Giá dự báo':<15}")
    print("-" * 35)
    list_ohlcvp = []
    list_ohlcvp.append(['timestamp', 'o', 'h', 'l', 'c', 'v', 'pred_price'])
    for day in range(1, 31):
        date_str = f"2025-09-{day:02d}"

        dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
        dt_utc = dt_obj.replace(tzinfo=timezone.utc)
        timestamp_s = dt_utc.timestamp()
        timestamp_ms = int(timestamp_s * 1000) - 1
        
        price = predict_next_price(ticker, timestamp_ms)
        ohlcv = get_future_data(ticker, timestamp_ms)
        list_ohlcvp.append([ohlcv.get('t'), ohlcv.get('o'), ohlcv.get('h'), ohlcv.get('l'), ohlcv.get('c'), ohlcv.get('v'), price])

        if price is not None:
            print(f"{date_str:<15} | {price:.2f}")
        else:
            print(f"{date_str:<15} | {'---':<15} (Thiếu Data/Lỗi)")

    with open('./data/output_lists.csv', mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerows(list_ohlcvp) # Ghi toàn bộ list một lúc

    print("Đã xuất file output_lists.csv thành công!")