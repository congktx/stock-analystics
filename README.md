# stock-analystics

## RUN API SERVER

1. cài đặt các package ở requirements.txt

2. cd api && copy nội dung .env.example vào file .env

3. chạy lệnh fastapi run main.py.

* xem các API của server tại http://0.0.0.0:8000/docs

# Quick Start

- Khởi động FastAPI:

```powershell
fastapi run dummy-data-source/main.py
```

- Vào `internal-database/src` và chạy (thay `year`/`month` trong mã nếu cần):

```powershell
cd .\internal-database\src
python init_data.py
python main.py
```

Lưu ý: chỉnh tham số năm/tháng trong `internal-database/src/main.py`
