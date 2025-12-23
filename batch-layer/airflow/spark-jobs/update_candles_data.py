from pyspark.sql import SparkSession
from pyspark.sql.functions import xxhash64, col, greatest, least, abs, lit, when,  dayofweek, month, quarter, year
import sys
spark = SparkSession.builder\
                    .appName("Update candles data")\
                    .master("yarn")\
                    .getOrCreate()
spark

start_date = sys.argv[1]
end_date = sys.argv[2]

df = spark.read.parquet("/airflow/data/ohlc") \
    .filter(
        f"ingest_date >= '{start_date}' "
        f"AND ingest_date < '{end_date}'"
    )

new_df = df.select(xxhash64(col("ticker")).alias("candle_company_id"),
                   xxhash64(col("ingest_date")).alias("candle_time_id"),
                   col("v").alias("candle_volume").cast("int"),
                   col("vw").alias("candle_volume_weighted"),
                   col("o").alias("candle_open"), 
                   col("c").alias("candle_close"), 
                   col("h").alias("candle_high"), 
                   col("l").alias("candle_low"),
                   col("n").alias("candle_num_of_trades").cast("int"))
ohlc_df = new_df.na.fill(0)
fact_candles = ohlc_df.withColumn("candle_upper_wick", col("candle_high") - greatest(col("candle_open"), col("candle_close")))\
                     .withColumn("candle_lower_wick", least(col("candle_open"), col("candle_close")) - col("candle_low"))\
                     .withColumn("candle_body_size", abs(col("candle_close") - col("candle_open")))\
                     .withColumn("candle_is_bullish", col("candle_close") > col("candle_open"))\
                     .withColumn("candle_typical_price", (col("candle_high") + col("candle_low") + col("candle_close"))/3)\
                     .withColumn("candle_avg_trade_size", when(col("candle_num_of_trades") > 0, col("candle_volume") / col("candle_num_of_trades")).otherwise(lit(0)))

jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=DWH;encrypt=false"

properties = {
    "user": "user",
    "password": "123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

fact_candles.write.jdbc(
    url=jdbc_url,
    table="fact_candles",
    mode="append",
    properties=properties
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

stg_dim_time.write.jdbc(
    url=jdbc_url,
    table="stg_dim_time",
    mode="overwrite",
    properties=properties
)
spark.stop()