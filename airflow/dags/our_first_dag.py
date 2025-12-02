from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'loind',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag',
    default_args=default_args,
    description="This is our first dag that we write",
    start_date=datetime(2025, 12, 1, 9),
    schedule='@daily'
    ) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )
    
    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey Im second task, running after first task!"
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo hey Im third task, running after first task!'
    )
    task1>>[task2, task3]
    # context = {'dag': dag}
    # task1.execute(context=context)