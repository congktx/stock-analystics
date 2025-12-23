from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count_distinct, count
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql.functions import xxhash64, col, dayofweek, month, quarter, year
import sys

jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=DWH;encrypt=false"

start_date = sys.argv[1] 
end_date = sys.argv[2]
properties = {
    "user": "user",
    "password": "123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

spark = SparkSession.builder\
                    .appName("Checking news data")\
                    .master("yarn")\
                    .getOrCreate()
spark
df = spark.read.parquet("/airflow/data/news") \
    .filter(f"ingest_date >= '{start_date}' AND ingest_date < '{end_date}'")

# dim_topics merge
topics_schema = ArrayType(
    StructType([
        StructField("topic", StringType(), True),
        StructField("relevance_score", StringType(), True)
    ])
)

df_parsed = df.withColumn("topics_parsed", from_json(col("topics_json"), topics_schema))
df_exploded = df_parsed.select(explode(col("topics_parsed")).alias("topic_struct"))
df_topics = df_exploded.select(col("topic_struct.topic").alias("topic"))
df_topics = df_topics.select(col("topic")).distinct()
dim_topics = df_topics.select(xxhash64(col("topic")).alias("topic_id"), col("topic").alias("topic_name"))
dim_topics.show()

dim_topics.write.jdbc(
    url=jdbc_url,
    table="stg_dim_topics",
    mode="overwrite",
    properties=properties
)

# dim_time merge
tmp_time = df.select(col("ingest_date")).distinct()
dim_time = tmp_time.select(xxhash64(col("ingest_date")).alias("time_id"),
                           col("ingest_date").alias("time_date"),
                           dayofweek(col("time_date")).alias("time_day_of_week"),
                           month(col("time_date")).alias("time_month"),
                           quarter(col("time_date")).alias("time_quarter"),
                           year(col("time_date")).alias("time_year"))
dim_time.show()
dim_time.write.jdbc(
    url=jdbc_url,
    table="stg_dim_time",
    mode="overwrite",
    properties=properties
)
# dim_news insert thẳng
dim_news = df.select(xxhash64(col("timestamp")).alias("news_id"),
                    xxhash64(col("ingest_date")).alias("news_time_id"),
                    col("overall_sentiment_score").alias("news_overall_score"),
                    col("title").alias("news_title"),
                    col("summary").alias("news_summary"),
                    col("category_within_source").alias("news_category_within_source"),
                    col("source").alias("news_source")).dropDuplicates(["news_id"])
dim_news.show(10)

dim_news.write.jdbc(
    url=jdbc_url,
    table="dim_news",
    mode="append",
    properties=properties
)
# fact_news_companies insert thẳng
schema = ArrayType(
    StructType([
        StructField("ticker", StringType(), True),
        StructField("relevance_score", StringType(), True),
        StructField("ticker_sentiment_score", StringType(), True)
    ])
)
tmp = df.select(xxhash64(col("timestamp")).alias("news_company_news_id"),
                from_json(col("ticker_sentiment_json"), schema).alias("ticker_sentiment"))
fact_news_companies = (
    tmp
    .withColumn("sentiment", explode(col("ticker_sentiment")))
    .select(
        col("news_company_news_id"),
        xxhash64(col("sentiment.ticker")).alias("news_company_company_id"),
        col("sentiment.relevance_score").alias("news_company_relevance_score"),
        col("sentiment.ticker_sentiment_score").alias("news_company_sentiment_score")
    )
)

fact_news_companies.show(10)
fact_news_companies.printSchema()
fact_news_companies.write.jdbc(
    url=jdbc_url,
    table="fact_news_companies",
    mode="append",
    properties=properties
)

# fact_news_topic insert thẳng
schema = ArrayType(
    StructType([
        StructField("topic", StringType(), True),
        StructField("relevance_score", StringType(), True)
    ])
)
tmp = df.select(xxhash64(col("timestamp")).alias("news_topic_news_id"), 
                from_json(col("topics_json"), schema).alias("topics"))

fact_news_topic = (
    tmp
    .withColumn("topics", explode(col("topics")))
    .select(
        col("news_topic_news_id"),
        xxhash64(col("topics.topic")).alias("news_topic_topic_id"),
        col("topics.relevance_score").alias("news_topic_relevance_score").cast("float")
    )
)

fact_news_topic.printSchema()
fact_news_topic.show(10)
fact_news_topic.write.jdbc(
    url=jdbc_url,
    table="fact_news_topic",
    mode="append",
    properties=properties
)
spark.stop()