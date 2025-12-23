# 📊 Stock Analytics - Hệ thống phân tích chứng khoán

Hệ thống Big Data phân tích dữ liệu chứng khoán theo thời gian thực, bao gồm các thành phần: thu thập dữ liệu (crawling), xử lý stream (Flink), batch processing (Spark), lưu trữ phân tán (HDFS), và dự đoán bằng Machine Learning.

## 📁 Cấu trúc thư mục

```
stock-analytics/
├── api/                        # FastAPI server - API truy vấn dữ liệu
│   ├── main.py                 # Entry point của API server
│   ├── router/                 # Các endpoint routes
│   ├── database/               # Kết nối MongoDB
│   └── util/                   # Utility functions
│
├── airflow/                    # Airflow standalone (development)
│   ├── dags/                   # Các DAG workflow
│   └── docker-compose.yml
│
├── batch-layer/                # Xử lý batch với HDFS + Spark + Airflow
│   ├── docker-compose.yaml     # Stack: Airflow + HDFS + Spark
│   ├── airflow/
│   │   ├── dags/               # DAG jobs (pull data, update DW, etc.)
│   │   └── spark-jobs/         # PySpark jobs
│   └── hdfs-spark/
│       └── config/             # Cấu hình Hadoop, Spark, Hive
│
├── config/                     # Cấu hình chung cho toàn hệ thống
│   ├── kafka_config.py         # Kafka configuration
│   └── settings.py             # Global settings
│
├── data-warehouse/             # Data Warehouse schema
│   └── script/
│       └── init.sql            # DDL cho dim tables & fact tables
│
├── deployment/                 # Deployment với Docker & K8s
│   ├── docker-compose.yml      # Stack: Kafka, Flink, Redis, TimescaleDB
│   └── k8s/                    # Kubernetes manifests
│
├── dummy-data-source/          # API giả lập nguồn dữ liệu
│   ├── main.py                 # FastAPI mock server
│   └── router/
│
├── flink-jobs/                 # Apache Flink streaming jobs
│   ├── news_processing_job.py  # Xử lý sentiment từ news
│   └── sinks/
│       └── mongodb_sink.py     # Sink dữ liệu vào MongoDB
│
├── hdfs/                       # HDFS standalone (development)
│   ├── docker-compose.yml
│   └── data/                   # Persistent data (namenode, datanode)
│
├── internal-database/          # Khởi tạo dữ liệu ban đầu
│   ├── data/                   # CSV/JSON source data
│   ├── scripts/
│   │   └── init.sql            # SQL init script
│   └── src/
│       ├── init_data.py        # Import dữ liệu ban đầu
│       └── main.py             # Pull company info từ API
│
├── machine-learning/           # ML models dự đoán giá cổ phiếu
│   ├── train_model.py          # Train PyTorch neural network
│   ├── inference.py            # Inference từ model đã train
│   └── model/
│       └── best_pytorch_model.pth
│
├── sql/                        # SQL schema cho analytics
│   └── news_analytics_schema.sql
│
├── src/                        # Source code dùng chung
│   ├── core/                   # Utility functions
│   ├── crawlers/               # Data crawlers
│   ├── storage/                # Storage adapters
│   └── streaming/              # Streaming utilities
│
├── stock-crawler-main/         # Crawler thu thập dữ liệu chứng khoán
│   ├── main.py                 # Entry point crawler
│   ├── run_pipeline.py         # Pipeline Kafka producer
│   ├── service/                # Các crawler services
│   │   ├── company_crawler.py  # Thông tin công ty
│   │   ├── market_crawler.py   # Thông tin thị trường
│   │   ├── news_crawler.py     # Tin tức & sentiment
│   │   └── ohlc_crawler.py     # Dữ liệu OHLC
│   └── utils/                  # Kafka producer, utilities
│
├── requirements.txt            # Python dependencies
└── run_stream_jobs.py          # Entry point cho stream jobs
```

---

## 🚀 Hướng dẫn chạy hệ thống

### Yêu cầu
- Python 3.10+
- Docker & Docker Compose
- 16GB RAM khuyến nghị

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

---

### 2. Khởi động Infrastructure (Kafka, Flink, Redis, TimescaleDB)

```bash
cd deployment
docker-compose up -d
```

**Các cổng được mở:**
| Service       | Port  | URL                        | Mô tả                     |
|---------------|-------|----------------------------|---------------------------|
| Kafka         | 9092  | -                          | Message broker            |
| Kafka UI      | 8080  | http://localhost:8080      | Quản lý Kafka topics      |
| Flink Web UI  | 8081  | http://localhost:8081      | Flink dashboard           |
| Redis         | 6379  | -                          | Caching                   |
| TimescaleDB   | 5432  | -                          | Time-series database      |

---

### 3. Khởi động Batch Layer (HDFS + Spark + Airflow)

```bash
cd batch-layer
docker-compose up -d
```

**Các cổng được mở:**
| Service              | Port  | URL                        | Mô tả                          |
|----------------------|-------|----------------------------|--------------------------------|
| Airflow Web UI       | 8080  | http://localhost:8080      | Quản lý DAGs                   |
| HDFS NameNode UI     | 9870  | http://localhost:9870      | HDFS file browser              |
| YARN ResourceManager | 8088  | http://localhost:8088      | YARN cluster status            |
| Spark History Server | 18080 | http://localhost:18080     | Spark job history              |
| Jupyter Lab          | 8888  | http://localhost:8888      | Notebook development           |
| MapReduce History    | 19888 | http://localhost:19888     | MapReduce job history          |

---

### 4. Chạy API Server

```bash
# Cấu hình environment
cd api
cp .env.example .env
# Chỉnh sửa .env với thông tin MongoDB, etc.

# Chạy server
fastapi run main.py
```

- API docs: http://localhost:8000/docs

---

### 5. Chạy Data Source (Mock API)

```bash
cd dummy-data-source
cp .env.example .env
fastapi run main.py
```

---

### 6. Khởi tạo dữ liệu ban đầu

```bash
cd internal-database/src

# Khởi tạo database schema
python init_data.py

# Pull dữ liệu công ty (chỉnh year/month trong main.py)
python main.py
```

---

### 7. Chạy Crawler thu thập dữ liệu

```bash
cd stock-crawler-main

# Crawl thông tin công ty theo năm/tháng
python main.py

# Hoặc chạy pipeline với Kafka
python run_pipeline.py
```

---

### 8. Machine Learning - Train Model

```bash
cd machine-learning

# Train model PyTorch
python train_model.py

# Inference
python inference.py
```

---

## 🏗️ Kiến trúc hệ thống

```
┌─────────────────┐      ┌─────────────────┐
│  Stock APIs     │      │  News APIs      │
│  (Alpha Vantage)│      │  (Sentiment)    │
└────────┬────────┘      └────────┬────────┘
         │                        │
         ▼                        ▼
┌─────────────────────────────────────────┐
│           Stock Crawler                 │
│  (company, market, ohlc, news crawler)  │
└────────────────┬────────────────────────┘
                 │
                 ▼
         ┌───────────────┐
         │     Kafka     │
         │  (Message Q)  │
         └───────┬───────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
┌───────────────┐  ┌──────────────────┐
│  Flink Jobs   │  │   Batch Layer    │
│  (Streaming)  │  │ (Airflow+Spark)  │
└───────┬───────┘  └────────┬─────────┘
        │                   │
        ▼                   ▼
┌───────────────┐  ┌──────────────────┐
│   MongoDB     │  │      HDFS        │
│ (Real-time)   │  │  (Data Lake)     │
└───────────────┘  └────────┬─────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │  Data Warehouse  │
                   │  (SQL Server)    │
                   └────────┬─────────┘
                            │
               ┌────────────┴────────────┐
               ▼                         ▼
      ┌─────────────────┐      ┌─────────────────┐
      │  FastAPI Server │      │  ML Models      │
      │  (REST API)     │      │  (PyTorch)      │
      └─────────────────┘      └─────────────────┘
```

---

## 📊 Data Warehouse Schema

### Dimension Tables
- `dim_companies` - Thông tin công ty (ticker, industry, CIK, etc.)
- `dim_time` - Dimension thời gian
- `dim_topics` - Chủ đề tin tức
- `dim_news` - Thông tin tin tức

### Fact Tables
- `fact_candles` - Dữ liệu OHLCV (Open, High, Low, Close, Volume)
- `news_sentiment_processed` - Sentiment đã xử lý
- `ticker_sentiment_daily` - Sentiment tổng hợp theo ngày

---

## 🔧 Cấu hình môi trường

### Kafka Config (`config/kafka_config.py`)
```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = {
    "news": "stock-news",
    "ohlc": "stock-ohlc",
    "company": "stock-company"
}
```

### API Config (`api/.env`)
```env
MONGODB_URI=mongodb://localhost:27017
DATABASE_NAME=stock_analytics
```

---

## 📝 Ghi chú

- **Lưu ý port conflict**: Batch layer và Deployment đều dùng port 8080 cho Airflow/Kafka UI. Chỉ chạy một trong hai cùng lúc hoặc đổi port.
- **HDFS**: Cần đợi HDFS khởi động hoàn tất (check qua UI port 9870) trước khi chạy Spark jobs.
- **Airflow DAGs**: Các DAG tự động được load từ `batch-layer/airflow/dags/`.

---

## 👥 Group 7

Dự án môn học Big Data - Phân tích chứng khoán thời gian thực.
