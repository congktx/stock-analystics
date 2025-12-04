from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import cursor as Cursor
from airflow.providers.standard.operators.python import PythonOperator
import csv
import logging

default_args = {
    'owner': 'loind',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_operator():
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    cursor: Cursor = conn.cursor()
    cursor.execute(
        """
        select * from public.orders where date <= '20220510'
        """
    )

    with open("./data/get_orders.txt", 'w') as f:
        csr_writer = csv.writer(f)
        csr_writer.writerow(cursor.description)
        csr_writer.writerows(cursor.fetchall())
    
    cursor.close()
    conn.close()
    logging.info("Saved orders data in text file get_orders.txt")

with DAG(
    dag_id='dag_with_postgres_hooks_v01',
    default_args=default_args,
    start_date=datetime(2025, 12, 1, 9),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = "postgres_operator",
        python_callable=postgres_operator
    )
    task1