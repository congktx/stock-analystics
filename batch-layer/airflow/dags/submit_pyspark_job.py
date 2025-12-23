from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'loind',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    dag_id="submit_pyspark_job",
    schedule=None,
    default_args=default_args
) as dag:
    task = SparkSubmitOperator(
        task_id="spark_submit",
        application="/workspace/airflow/spark-jobs/sample.py",
        conn_id="spark",
        jars='/workspace/airflow/connector/mssql-jdbc-12.2.0.jre11.jar',
        deploy_mode='cluster',
        verbose=True
    )
    # spark-submit --master yarn  --deploy-mode cluster --jars /workspace/airflow/connector/mssql-jdbc-12.2.0.jre11.jar --verbose  /workspace/airflow/spark-jobs/sample.py
    # spark-submit --master yarn  --deploy-mode cluster  --verbose  /workspace/src/sample.py