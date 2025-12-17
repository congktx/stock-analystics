from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import xxhash64, col, sum
spark = SparkSession.builder\
                    .appName("Update dim_companies table")\
                    .master('yarn')\
                    .getOrCreate()
spark

jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=DWH;encrypt=false"

properties = {
    "user": "user",
    "password": "123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

schema = StructType([
    StructField("company_name", StringType()),
    StructField("company_ticker", StringType()),
    StructField("company_asset_type", StringType()),
    StructField("company_composite_figi", StringType()),
    StructField("company_cik", StringType()),
    StructField("company_industry", StringType()),
    StructField("company_sic_code", StringType())
])

companies = spark.read.schema(schema).format("parquet").load("/airflow/data/companies")
companies = companies.select(xxhash64(col("company_ticker")).alias("company_id"),
                 "company_name",
                 "company_ticker",
                 col("company_asset_type").alias("company_security_type"),
                "company_composite_figi",
                "company_cik",
                col("company_industry").alias("company_industry_name"),
                col("company_sic_code").alias("company_sic"))
companies.printSchema()
companies = companies.na.fill("Unknown")
new_null_df = companies.select([sum(col(c).isNull().cast("int")).alias(f"{c}_nulls") for c in companies.columns])
new_null_df.show()
companies.write.jdbc(
    url=jdbc_url,
    table="dim_companies",
    mode="overwrite",
    properties=properties
)
spark.stop()