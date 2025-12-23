# Batch Layer — Quick Docker Compose Guide

Short guide để khởi động stack `batch-layer` bằng `docker-compose` và biết các cổng (ports) đã map ra ngoài dùng để làm gì.

## Khởi động
- Từ thư mục `batch-layer` chạy:

```powershell
docker-compose up -d
```

- Kiểm tra trạng thái:

```powershell
docker-compose ps
```

- Xem logs (theo dõi realtime):

```powershell
docker-compose logs -f <service-name>
# ví dụ: docker-compose logs -f airflow
```

## Dừng và xóa containers

```powershell
docker-compose down
```

## Ports (host:container) và công dụng
- `8080:8080` — Airflow web UI. Mở `http://localhost:8080` để truy cập giao diện quản lý DAGs và tasks.

- Namenode / Hadoop / Spark related (exposed từ service `namenode`):
  - `8088:8088` — YARN ResourceManager UI (theo mặc định). Dùng để xem ứng dụng YARN, trạng thái cluster.
  - `19888:19888` — MapReduce HistoryServer UI. Dùng để xem lịch sử job MapReduce và logs của các job cũ.
  - `9870:9870` — HDFS NameNode web UI. Dùng để kiểm tra trạng thái HDFS, datanodes, và phân vùng lưu trữ.
  - `8888:8888` — Jupyter Lab. Mở `http://localhost:8888` để truy cập notebooks (đã chạy trong container).
  - `18080:18080` — Spark History Server UI. Dùng để xem lịch sử ứng dụng Spark (event logs lưu tại `spark-events`).
  - `15002:15002` — cổng bổ sung (dịch vụ tùy biến). Nếu cần biết chính xác mục đích, xem logs của container `namenode` hoặc cấu hình trong `hdfs-spark/config`.
