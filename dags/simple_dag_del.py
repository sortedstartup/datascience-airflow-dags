from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

def print_hello():
    print("Hello from Airflow")

def wait():
    time.sleep(5)
    print("Waited for 5 seconds")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='simple_test_dag_1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['test']
) as dag:  # ← this line was missing the closing parenthesis earlier

    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    task2 = PythonOperator(
        task_id='wait_task',
        python_callable=wait
    )

    task1 >> task2
