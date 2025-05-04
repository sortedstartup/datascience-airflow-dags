from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

# Define Python functions
def print_hello():
    print("Hello from Airflow")

def wait():
    time.sleep(5)
    print("Waited for 5 seconds")

# Default DAG args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define DAG
with DAG(
    dag_id='simple_test_dag',
    default_args_
