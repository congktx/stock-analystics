from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from utils import (pull_daily_ohlc_data, 
                   date_to_timestamp,
                   make_date)
from datetime import datetime, timedelta
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from hdfs import InsecureClient
import pandas as pd

default_args = {
    "owner": "loind",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

def pull_data_daily_job(**context):
    start_date:datetime = context["data_interval_start"]
    from_timestamp = date_to_timestamp(year=start_date.year, 
                                       month=start_date.month,
                                       day=start_date.day,
                                       second=1)
    start_date += timedelta(days=1)
    to_timestamp = date_to_timestamp(year=start_date.year, 
                                     month=start_date.month,
                                     day=start_date.day)
    
    pull_daily_ohlc_data(from_timestamp=from_timestamp,
                    to_timestamp=to_timestamp)

def upload_file_into_hdfs(**context):
    hook = WebHDFSHook(webhdfs_conn_id='hdfs')
    start_date:datetime = context["data_interval_start"]
    folder = f"ingest_date={make_date(year=start_date.year,
                                      month=start_date.month,
                                      day=start_date.day)}"
    conn: InsecureClient = hook.get_conn()
    conn.makedirs(f"/airflow/data/ohlc/{folder}")
    hook.load_file(source='/workspace/airflow/data/output.parquet',
                   destination=f'/airflow/data/ohlc/{folder}/data.parquet')
    
with DAG(
    dag_id="pull_daily_ohlc_data_into_hdfs",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule="@daily"
) as dag:
    task1 = PythonOperator(
        task_id = "pull_ohlc_data_from_source",
        python_callable=pull_data_daily_job
    )

    task2 = PythonOperator(
        task_id = "upload_file_into_hdfs",
        python_callable=upload_file_into_hdfs
    )

    task1 >> task2