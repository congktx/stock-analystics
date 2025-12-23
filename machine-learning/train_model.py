# FILE: 4_deep_learning_pytorch.py
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import gc
import os
import time

# --- CẤU HÌNH ---
INPUT_FILE = "/kaggle/input/bigdata-ohlcv/training_data_returns.csv"
BEST_MODEL_FILE = "best_pytorch_model.pth"
SCALER_FILE = "scaler.pkl" # Cần lưu bộ chuẩn hóa để dùng lại khi inference

# Hyperparameters (Siêu tham số)
BATCH_SIZE = 1024       # Số lượng mẫu học 1 lần (Tăng nếu GPU mạnh, giảm nếu hết VRAM)
LEARNING_RATE = 0.001   # Tốc độ học
EPOCHS = 100            # Số lần học lặp lại toàn bộ dữ liệu
PATIENCE = 10           # Nếu 10 epochs không khá hơn thì dừng (Early Stopping)
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

print(f"--- Đang sử dụng thiết bị: {DEVICE} ---")

# =========================================================
# 1. TẢI VÀ XỬ LÝ DỮ LIỆU
# =========================================================
print(f"--- Đang tải dữ liệu từ {INPUT_FILE} ---")
try:
    df = pd.read_csv(INPUT_FILE, dtype=np.float32)
    # df = df[(df.iloc[:, -1] > -0.5) & (df.iloc[:, -1] < 0.5)] # Lọc nhiễu nếu cần
    
    X = df.iloc[:, :-1].values
    y = df.iloc[:, -1].values
    
    del df
    gc.collect()
except FileNotFoundError:
    print("Lỗi file.")
    exit()

# ---------------------------------------------------------
# 2. CHIA TẬP TRAIN - VALID - TEST
# ---------------------------------------------------------
print("--- Đang chia dữ liệu ---")
X_temp, X_test, y_temp, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train, X_val, y_train, y_val = train_test_split(X_temp, y_temp, test_size=0.25, random_state=42)

# ---------------------------------------------------------
# 3. CHUẨN HÓA DỮ LIỆU (BẮT BUỘC VỚI DEEP LEARNING)
# ---------------------------------------------------------
print("--- Đang chuẩn hóa dữ liệu (StandardScaler) ---")
scaler = StandardScaler()
# Chỉ fit trên tập Train để tránh lộ thông tin (Data Leakage)
X_train = scaler.fit_transform(X_train)
X_val = scaler.transform(X_val)
X_test = scaler.transform(X_test)

# Lưu scaler để dùng cho inference sau này
joblib.dump(scaler, SCALER_FILE)

# Chuyển sang PyTorch Tensor
X_train_t = torch.tensor(X_train, dtype=torch.float32).to(DEVICE)
y_train_t = torch.tensor(y_train, dtype=torch.float32).view(-1, 1).to(DEVICE)

X_val_t = torch.tensor(X_val, dtype=torch.float32).to(DEVICE)
y_val_t = torch.tensor(y_val, dtype=torch.float32).view(-1, 1).to(DEVICE)

X_test_t = torch.tensor(X_test, dtype=torch.float32).to(DEVICE)
y_test_t = torch.tensor(y_test, dtype=torch.float32).view(-1, 1).to(DEVICE)

# Tạo DataLoader để batching
train_dataset = TensorDataset(X_train_t, y_train_t)
train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)

# =========================================================
# 4. ĐỊNH NGHĨA MÔ HÌNH (NEURAL NETWORK)
# =========================================================

class StockPredictor(nn.Module):
    def __init__(self, input_dim):
        super(StockPredictor, self).__init__()
        
        # Mạng nơ-ron dạng tháp (giảm dần số nơ-ron)
        self.net = nn.Sequential(
            # Layer 1: Input -> 256
            nn.Linear(input_dim, 256),
            nn.BatchNorm1d(256), # Giúp hội tụ nhanh, ổn định
            nn.ReLU(),
            nn.Dropout(0.3),     # Tắt ngẫu nhiên 30% nơ-ron để chống học vẹt

            # Layer 2: 256 -> 128
            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.2),

            # Layer 3: 128 -> 64
            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            
            # Layer 4: Output (1 giá trị dự đoán)
            nn.Linear(64, 1) 
        )

    def forward(self, x):
        return self.net(x)

model = StockPredictor(input_dim=X_train.shape[1]).to(DEVICE)

# Hàm mất mát (Loss) và Tối ưu (Optimizer)
criterion = nn.MSELoss() # Dùng MSE cho bài toán hồi quy
optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)

# =========================================================
# 5. QUÁ TRÌNH HUẤN LUYỆN (TRAINING LOOP)
# =========================================================
print(f"\n--- Bắt đầu Train ({EPOCHS} Epochs) ---")

best_val_loss = float('inf')
patience_counter = 0

for epoch in range(EPOCHS):
    model.train() # Chế độ train (bật Dropout)
    train_loss = 0.0
    
    for inputs, targets in train_loader:
        optimizer.zero_grad()           # Xóa gradient cũ
        outputs = model(inputs)         # Dự đoán (Forward)
        loss = criterion(outputs, targets) # Tính lỗi
        loss.backward()                 # Lan truyền ngược (Backward)
        optimizer.step()                # Cập nhật trọng số
        train_loss += loss.item() * inputs.size(0)
    
    train_loss /= len(train_loader.dataset)
    
    # Đánh giá trên tập Valid
    model.eval() # Chế độ eval (tắt Dropout)
    with torch.no_grad():
        val_outputs = model(X_val_t)
        val_loss = criterion(val_outputs, y_val_t).item()
        
    print(f"Epoch {epoch+1}/{EPOCHS} | Train Loss: {train_loss:.6f} | Val Loss: {val_loss:.6f}")
    
    # --- EARLY STOPPING CHECK ---
    if val_loss < best_val_loss:
        best_val_loss = val_loss
        patience_counter = 0
        # Lưu model tốt nhất
        torch.save(model.state_dict(), BEST_MODEL_FILE)
        # print("  -> Model improved. Saved!")
    else:
        patience_counter += 1
        if patience_counter >= PATIENCE:
            print(f"\nDừng sớm (Early Stopping) tại epoch {epoch+1} vì Val Loss không giảm nữa.")
            break

# =========================================================
# 6. ĐÁNH GIÁ CUỐI CÙNG TRÊN TẬP TEST
# =========================================================
print("\n" + "="*50)
print("KẾT QUẢ CUỐI CÙNG TRÊN TẬP TEST")
print("="*50)

# Load lại trọng số tốt nhất đã lưu
model.load_state_dict(torch.load(BEST_MODEL_FILE))
model.eval()

with torch.no_grad():
    y_test_pred_t = model(X_test_t)
    y_test_pred = y_test_pred_t.cpu().numpy().flatten() # Chuyển về CPU numpy

# Tính toán Metrics
test_mse = mean_squared_error(y_test, y_test_pred)
test_mae = mean_absolute_error(y_test, y_test_pred)
test_r2 = r2_score(y_test, y_test_pred)

print(f"Test MSE : {test_mse:.6f}")
print(f"Test MAE : {test_mae:.4f}")
print(f"Test R2  : {test_r2:.4f}")

# --- Hiển thị Mẫu ---
results = pd.DataFrame({'Actual': y_test, 'Predicted': y_test_pred})
results['Abs_Error'] = (results['Actual'] - results['Predicted']).abs()
view_df = pd.DataFrame()
view_df['Thực tế (%)'] = results['Actual'] * 100
view_df['Dự đoán (%)'] = results['Predicted'] * 100
view_df['Sai lệch (%)'] = view_df['Thực tế (%)'] - view_df['Dự đoán (%)']
view_df['Sai số tuyệt đối'] = results['Abs_Error']

print("\n--- 5 Mẫu Ngẫu Nhiên ---")
print(view_df.drop(columns=['Sai số tuyệt đối']).sample(5))

print("\n--- 5 Mẫu Dự Đoán Tốt Nhất ---")
print(view_df.sort_values(by='Sai số tuyệt đối', ascending=True).head(5).drop(columns=['Sai số tuyệt đối']))

print(f"\n--- Đã lưu Model tốt nhất vào: {BEST_MODEL_FILE} ---")
print(f"--- Đã lưu Scaler vào: {SCALER_FILE} (Cần dùng cho inference) ---")