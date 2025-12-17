from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder\
                    .appName("Test mssql connection")\
                    .master('yarn')\
                    .getOrCreate()
spark

jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=test;encrypt=false"

properties = {
    "user": "user",
    "password": "123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", StringType(), True)
])

data = [
    (1, 'Alice Johnson', 'HR', 5000, '2021-03-15'),
    (2, 'Bob Smith', 'IT', 7000, '2020-06-01'),
    (3, 'Charlie Brown', 'Finance', 6500, '2019-09-12'),
    (4, 'Diana Prince', 'Marketing', 6000, '2022-01-10'),
    (5, 'Ethan Hunt', 'IT', 7200, '2021-11-05'),
    (6, 'Fiona Gallagher', 'HR', 5200, '2020-08-21'),
    (7, 'George Martin', 'Finance', 6700, '2019-12-30'),
    (8, 'Hannah Baker', 'Marketing', 6100, '2021-05-18'),
    (9, 'Ian Fleming', 'IT', 7300, '2022-02-14'),
    (10, 'Julia Roberts', 'HR', 5400, '2021-09-25'),
    (11, 'Kevin Durant', 'Finance', 6800, '2020-03-11'),
    (12, 'Laura Palmer', 'Marketing', 6200, '2019-07-04'),
    (13, 'Michael Jordan', 'IT', 7500, '2020-12-20'),
    (14, 'Nancy Drew', 'HR', 5500, '2022-06-01'),
    (15, 'Oscar Wilde', 'Finance', 6900, '2021-04-15'),
    (16, 'Pam Beesly', 'Marketing', 6300, '2020-11-10'),
    (17, 'Quentin Tarantino', 'IT', 7700, '2019-10-05'),
    (18, 'Rachel Green', 'HR', 5600, '2021-08-30'),
    (19, 'Steve Rogers', 'Finance', 7000, '2020-01-22'),
    (20, 'Tina Fey', 'Marketing', 6400, '2019-05-17')
]

df = spark.createDataFrame(data, schema=schema)
df.show()

df.write.jdbc(
    url=jdbc_url,
    table="employee",
    mode="overwrite",
    properties=properties
)
spark.stop()