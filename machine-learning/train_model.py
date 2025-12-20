# FILE: 4_manual_tuning_full_metrics.py
import pandas as pd
import numpy as np
import cupy as cp
from cuml.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, ParameterSampler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import gc

# --- CẤU HÌNH ---
INPUT_FILE = "/kaggle/input/bigdata-ohlcv/training_data_returns.csv"
BEST_MODEL_FILE = "best_cuml_rf_model.pkl"

print(f"--- Đang tải dữ liệu từ {INPUT_FILE} ---")
try:
    # Load float32 để tiết kiệm RAM
    df = pd.read_csv(INPUT_FILE, dtype=np.float32)
    # Lọc lại dữ liệu (như đã bàn ở các bước trước)
    # df = df[(df.iloc[:, -1] > -0.5) & (df.iloc[:, -1] < 0.5)] 
    
    X = df.iloc[:, :-1].values
    y = df.iloc[:, -1].values
    
    del df
    gc.collect()
except FileNotFoundError:
    print("Lỗi file.")
    exit()

# ---------------------------------------------------------
# 1. CHIA 3 TẬP: TRAIN (60%) - VALID (20%) - TEST (20%)
# ---------------------------------------------------------
print("--- Đang chia dữ liệu ---")

X_temp, X_test, y_temp, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, shuffle=True
)

X_train, X_val, y_train, y_val = train_test_split(
    X_temp, y_temp, test_size=0.25, random_state=42, shuffle=True
)

del X, y, X_temp, y_temp
gc.collect()

print(f"-> Train: {X_train.shape}")
print(f"-> Valid: {X_val.shape}")
print(f"-> Test : {X_test.shape}")

# ---------------------------------------------------------
# 2. CẤU HÌNH KHÔNG GIAN THAM SỐ
# ---------------------------------------------------------
param_grid = {
    'n_estimators': [100, 300],
    'max_depth': [16, 20, 24],
    'min_impurity_decrease': [0.0, 0.00001],
    'min_samples_leaf': [1, 3, 5]
}

param_list = list(ParameterSampler(param_grid, n_iter=10, random_state=42))

print(f"\n--- Bắt đầu vòng lặp tìm kiếm (Thử {len(param_list)} cấu hình) ---")

best_score = -float('inf')
best_params = None

# ---------------------------------------------------------
# 3. VÒNG LẶP: SO SÁNH LOSS TRAIN VS LOSS VALID
# ---------------------------------------------------------
for i, params in enumerate(param_list):
    print(f"\n[{i+1}/{len(param_list)}] Params: {params}")
    
    model = RandomForestRegressor(
        **params,
        random_state=42,
        n_streams=1,
        verbose=False
    )
    
    # Train
    model.fit(X_train, y_train)
    
    # --- [MỚI] TÍNH LOSS TRÊN TẬP TRAIN ---
    y_train_pred = model.predict(X_train)
    if isinstance(y_train_pred, cp.ndarray): y_train_pred = y_train_pred.get()
    
    train_mse = mean_squared_error(y_train, y_train_pred)
    train_r2 = r2_score(y_train, y_train_pred)
    
    # --- TÍNH LOSS TRÊN TẬP VALID ---
    y_val_pred = model.predict(X_val)
    if isinstance(y_val_pred, cp.ndarray): y_val_pred = y_val_pred.get()
    
    val_mse = mean_squared_error(y_val, y_val_pred)
    val_r2 = r2_score(y_val, y_val_pred)
    
    # In ra để so sánh
    print(f"   -> Train MSE: {train_mse:.6f} | R2: {train_r2:.4f}")
    print(f"   -> Valid MSE: {val_mse:.6f} | R2: {val_r2:.4f}")
    
    # Kiểm tra chênh lệch (Overfitting check)
    diff_mse = abs(train_mse - val_mse)
    print(f"   -> Chênh lệch MSE: {diff_mse:.6f}")

    if val_r2 > best_score:
        best_score = val_r2
        best_params = params
        print("   => (Mô hình này đang tốt nhất!)")

print("\n" + "="*50)
print(f"THAM SỐ TỐT NHẤT: {best_params}")
print(f"Best Valid R2: {best_score:.4f}")
print("="*50)

# ---------------------------------------------------------
# 4. RETRAIN & ĐÁNH GIÁ TRÊN TẬP TRAIN (GỘP)
# ---------------------------------------------------------
print("\n--- Gộp Train + Valid để huấn luyện mô hình cuối cùng ---")

X_final_train = np.concatenate((X_train, X_val), axis=0)
y_final_train = np.concatenate((y_train, y_val), axis=0)

# Xóa bớt biến cũ
del X_train, y_train, X_val, y_val, model
gc.collect()

final_model = RandomForestRegressor(
    **best_params,
    random_state=42,
    n_streams=1,
    verbose=True
)

final_model.fit(X_final_train, y_final_train)

# --- [MỚI] ĐÁNH GIÁ TRÊN TẬP TRAIN CUỐI CÙNG ---
print("\n" + "-"*30)
print("ĐÁNH GIÁ TRÊN TẬP TRAIN (Đã gộp)")
print("-"*30)
y_final_train_pred = final_model.predict(X_final_train)
if isinstance(y_final_train_pred, cp.ndarray): y_final_train_pred = y_final_train_pred.get()

ft_mse = mean_squared_error(y_final_train, y_final_train_pred)
ft_mae = mean_absolute_error(y_final_train, y_final_train_pred)
ft_r2 = r2_score(y_final_train, y_final_train_pred)

print(f"Train MSE : {ft_mse:.6f}")
print(f"Train MAE : {ft_mae:.4f}")
print(f"Train R2  : {ft_r2:.4f}")

# ---------------------------------------------------------
# 5. ĐÁNH GIÁ TRÊN TẬP TEST
# ---------------------------------------------------------
print("\n" + "="*30)
print("KẾT QUẢ CUỐI CÙNG TRÊN TẬP TEST")
print("="*30)

y_test_pred = final_model.predict(X_test)
if isinstance(y_test_pred, cp.ndarray): y_test_pred = y_test_pred.get()

test_mse = mean_squared_error(y_test, y_test_pred)
test_mae = mean_absolute_error(y_test, y_test_pred)
test_r2 = r2_score(y_test, y_test_pred)

print(f"Test MSE : {test_mse:.6f}")
print(f"Test MAE : {test_mae:.4f}")
print(f"Test R2  : {test_r2:.4f}")

# --- PHẦN HIỂN THỊ MẪU ---
results = pd.DataFrame({'Actual': y_test, 'Predicted': y_test_pred})
results['Abs_Error'] = (results['Actual'] - results['Predicted']).abs()

view_df = pd.DataFrame()
view_df['Thực tế (%)'] = results['Actual'] * 100
view_df['Dự đoán (%)'] = results['Predicted'] * 100
view_df['Sai lệch (%)'] = view_df['Thực tế (%)'] - view_df['Dự đoán (%)']
view_df['Sai số tuyệt đối'] = results['Abs_Error']

print("\n--- [1] 5 Mẫu Ngẫu Nhiên ---")
print(view_df.drop(columns=['Sai số tuyệt đối']).sample(5))

print("\n--- [2] 5 Mẫu Dự Đoán TỐT NHẤT (Sai lệch ~ 0) ---")
print(view_df.sort_values(by='Sai số tuyệt đối', ascending=True).head(5).drop(columns=['Sai số tuyệt đối']))

print("\n--- [3] 5 Mẫu Dự Đoán TỆ NHẤT (Sai lệch lớn) ---")
print(view_df.sort_values(by='Sai số tuyệt đối', ascending=False).head(5).drop(columns=['Sai số tuyệt đối']))

# In thêm các mẫu dự đoán giảm
neg_pred = view_df[view_df['Dự đoán (%)'] < 0]
print(f"\n--- [4] 5/{len(neg_pred)} Mẫu Model Dự đoán GIẢM GIÁ (< 0) ---")
if len(neg_pred) > 0:
    print(neg_pred.drop(columns=['Sai số tuyệt đối']).sample(min(5, len(neg_pred))))

print(f"\n--- Lưu model vào {BEST_MODEL_FILE} ---")
joblib.dump(final_model, BEST_MODEL_FILE)