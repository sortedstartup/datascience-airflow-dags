from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
from collections import Counter
import requests
import logging
import socket

HES_URL = "http://hes-mock.mock/api/readings/bulk"
MDM_URL = "http://mdm-mock.mock/api/mdm/readings"

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "total_meters": 300000,
        "batch_size": 150
    },
    tags=["hes", "mdm"],
)
def hes_mdm_reduced_batches_3():

    @task
    def generate_batches():
        total_meters = int(params.get("total_meters", 300000))
        batch_size = int(params.get("batch_size", 150))

        meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, total_meters + 1)]
        batches = [meter_ids[i:i + batch_size] for i in range(0, len(meter_ids), batch_size)]
        logging.info(f"Generated {len(batches)} batches of {batch_size} meters each.")
        return batches

    @task
    def process_batch_bulk(meter_ids):
        hostname = socket.gethostname()
        logging.info(f"[{hostname}] Processing batch with {len(meter_ids)} meters. First meter: {meter_ids[0]}")

        # Simulated API requests
        hes_resp = requests.post(HES_URL, json={"meterIds": meter_ids}, timeout=30)
        hes_resp.raise_for_status()
        bulk_data = hes_resp.json().get("bulkReadings", [])
        logging.info(f"[{hostname}] Received {len(bulk_data)} records from HES.")

        payload = [{"meterId": m["meterId"], "readings": m["readings"]} for m in bulk_data]
        mdm_resp = requests.post(MDM_URL, json=payload, timeout=30)
        mdm_resp.raise_for_status()
        logging.info(f"[{hostname}] Sent {len(payload)} records to MDM.")

        return hostname

    @task
    def summarize_worker_usage(hostnames):
        count = Counter(hostnames)
        for worker, num_tasks in count.items():
            print(f"Worker '{worker}' ran {num_tasks} tasks")

    batches = generate_batches()
    worker_hosts = process_batch_bulk.expand(meter_ids=batches)
    summarize_worker_usage(worker_hosts)

dag = hes_mdm_reduced_batches_3()
