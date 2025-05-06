
# dags/benchmark_with_logs.py

import random
import time
import socket
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging
from collections import defaultdict

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='benchmark_with_logs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_tasks=48,
    description='Benchmark with full logging',
) as dag:

    @task
    def generate_data():
        return [{"task_id": f"task_{i}", "value": f"value_{i}"} for i in range(1000)]

    @task
    def run_subtask(task_data):
        start_time = time.time()
        hostname = socket.gethostname()
        time.sleep(random.uniform(0.05, 0.2))  # Simulated work
        duration = time.time() - start_time
        logging.info(f"[{task_data['task_id']}] ran on [{hostname}], value={task_data['value']}, duration={duration:.2f}s")
        return hostname

    @task
    def summarize(hostnames: list):
        start = time.time()
        count = defaultdict(int)
        for host in hostnames:
            count[host] += 1

        logging.info("Task Distribution by Worker:")
        for host, num_tasks in count.items():
            logging.info(f"  Worker {host}: {num_tasks} tasks")

        logging.info(f"Total workers: {len(count)}")
        logging.info(f"Total tasks: {len(hostnames)}")
        logging.info(f"Summary duration: {time.time() - start:.2f}s")

    task_list = generate_data()
    hostnames = run_subtask.expand(task_data=task_list)
    summarize(hostnames)
