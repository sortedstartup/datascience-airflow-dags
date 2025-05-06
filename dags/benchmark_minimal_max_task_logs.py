
# dags/benchmark_minimal_logs.py

import random
import time
import socket
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from collections import defaultdict

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='benchmark_minimal_logs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_tasks=48,
    description='Benchmark with minimal logging',
) as dag:

    @task
    def generate_data():
        return [{"task_id": f"task_{i}", "value": f"value_{i}"} for i in range(1000)]

    @task
    def run_subtask(task_data):
        start_time = time.time()
        hostname = socket.gethostname()
        time.sleep(random.uniform(0.05, 0.2))
        duration = time.time() - start_time
        return {"host": hostname, "duration": duration}

    @task
    def summarize(results: list):
        from collections import defaultdict
        count = defaultdict(int)
        total_time = 0.0
        for res in results:
            count[res["host"]] += 1
            total_time += res["duration"]

        print(f"Summary: {dict(count)}")
        print(f"Total tasks: {len(results)}")
        print(f"Total time taken (sum of all task durations): {total_time:.2f}s")

    task_list = generate_data()
    results = run_subtask.expand(task_data=task_list)
    summarize(results)
