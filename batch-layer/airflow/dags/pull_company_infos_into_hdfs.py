from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from hdfs import InsecureClient
import csv
import logging
from utils import pull_companies_data

default_args = {
    'owner': 'loind',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def pull_companies_data_operator():
    pull_companies_data()

def upload_companies_data_into_hdfs():
    hook = WebHDFSHook(webhdfs_conn_id="hdfs")
    hook.load_file(source='/workspace/airflow/data/companies.parquet',
                   destination=f'/airflow/data/companies/data.parquet')

with DAG(
    dag_id='pull_company_infos_into_hdfs',
    default_args=default_args,
    start_date=datetime(2025, 12, 1, 9),
    schedule=None
) as dag:
    task1 = PythonOperator(
        task_id = "pull_companies_data_operator",
        python_callable=pull_companies_data_operator
    )
    task2 = PythonOperator(
        task_id = "upload_companies_data_into_hdfs",
        python_callable=upload_companies_data_into_hdfs
    )
    task1 >> task2