from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Intentional error: Missing parentheses in DAG definition
with DAG(
    dag_id='broken_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
# Missing closing parenthesis here 👇
    default_args={'owner': 'airflow'}
:  # <-- invalid colon here instead of ')'

    def fail_task():
        print("This should never run")

    task = PythonOperator(
        task_id='fail',
        python_callable=fail_task
    )
