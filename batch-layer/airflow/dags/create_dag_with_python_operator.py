from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.standard.operators.python import PythonOperator
from typing import Dict
from pprint import pprint

default_args = {
    'owner': 'loind',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(some_dict: Dict, ti):
    print("some dict: ", some_dict)
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')

    print(f"Hello World! Myname is {first_name} {last_name}, and I am {age} years old!")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')

def get_age(ti):
    ti.xcom_push(key='age', value=19)

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v01',
    description='Our first dag using python operator',
    start_date=datetime(2025, 12, 1, 9),
    schedule='@daily'
) as dag:

    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'some_dict': {'a':1, 'b': 2}}
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1