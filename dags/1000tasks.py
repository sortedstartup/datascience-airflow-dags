# dags/one_script_dynamic_dag.py

import random
import time
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dynamic_json_tasks_in_memory',
    default_args=default_args,
    description='Generate JSON and run 1000 tasks in memory',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=100,  # Tune for performance
) as dag:

    @task
    def generate_data():
        # Generate 1000 in-memory dicts
        return [{"task_id": f"task_{i}", "value": f"value_{i}"} for i in range(1000)]

    @task
    def run_subtask(task_data):
        print(f"Running {task_data['task_id']} with {task_data['value']}")

    # Generate → Map to Subtasks
    task_list = generate_data()
    run_subtask.expand(task_data=task_list)
