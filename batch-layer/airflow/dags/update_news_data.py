from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'loind',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    dag_id="update_news_data",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    default_args=default_args
) as dag:
    task1 = SparkSubmitOperator(
        task_id="submit_update_news_data_job",
        application="/workspace/airflow/spark-jobs/update_news_data.py",
        conn_id="spark",
        jars='/workspace/airflow/connector/mssql-jdbc-12.2.0.jre11.jar',
        deploy_mode='cluster',
        verbose=True,
        application_args=[
            "{{ macros.ds_add(ds, -7) }}",  
            "{{ ds }}",                     
        ]
    )

    task2 = SQLExecuteQueryOperator(
        task_id = "merge_data_from_stg_dim_topics_into_dim_topics",
        conn_id= "mssql",
        sql="""
            MERGE dim_topics AS d
            USING stg_dim_topics AS s
            ON d.topic_id = s.topic_id
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    topic_id,
                    topic_name
                )
                VALUES (
                    s.topic_id,
                    s.topic_name
                );
            """
    )
    task3 = SQLExecuteQueryOperator(
        task_id = "merge_data_from_stg_dim_time_into_dim_time",
        conn_id= "mssql",
        sql="""
            MERGE dim_time AS d
            USING stg_dim_time AS s
            ON d.time_id = s.time_id
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    time_id,
                    time_date,
                    time_day_of_week,
                    time_month,
                    time_quarter,
                    time_year
                )
                VALUES (
                    s.time_id,
                    s.time_date,
                    s.time_day_of_week,
                    s.time_month,
                    s.time_quarter,
                    s.time_year
                );
            """
    )
    task1 >> task2 >> task3
    # spark-submit --master yarn  --deploy-mode cluster --jars /workspace/airflow/connector/mssql-jdbc-12.2.0.jre11.jar --verbose  /workspace/airflow/spark-jobs/sample.py
    # spark-submit --master yarn  --deploy-mode cluster  --verbose  /workspace/src/sample.py