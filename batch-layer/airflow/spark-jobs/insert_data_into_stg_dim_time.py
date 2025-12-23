from pyspark.sql import SparkSession
from pyspark.sql.functions import xxhash64, col, dayofweek, month, quarter, year
import sys

spark = SparkSession.builder \
    .appName("Update dim_time table") \
    .master("yarn") \
    .getOrCreate()

start_date = sys.argv[1]
end_date = sys.argv[2]

df = spark.read.parquet("/airflow/data/ohlc") \
    .filter(
        f"ingest_date >= '{start_date}' "
        f"AND ingest_date < '{end_date}'"
    )

time_df = df.select(
    xxhash64(col("ingest_date")).alias("time_id"),
    col("ingest_date").alias("time_date")
).distinct()

stg_dim_time = time_df.select(
    "*",
    dayofweek(col("time_date")).alias("time_day_of_week"),
    month(col("time_date")).alias("time_month"),
    quarter(col("time_date")).alias("time_quarter"),
    year(col("time_date")).alias("time_year")
)

jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=DWH;encrypt=false"

properties = {
    "user": "user",
    "password": "123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


stg_dim_time.write.jdbc(
    url=jdbc_url,
    table="stg_dim_time",
    mode="append",
    properties=properties
)

spark.stop()
