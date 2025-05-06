from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import logging
import socket
import math

HES_URL = "http://hes-mock.mock/api/readings/bulk"
MDM_URL = "http://mdm-mock.mock/api/mdm/readings"

@dag(
    dag_id="hes_mdm_reduced_batches_hardcoded_chunks",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["hes", "mdm"],
)
def hes_mdm_reduced_batches():

    @task
    def generate_batches():
        total_meters = 300000
        batch_size = 150
        max_per_expand = 1024

        meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, total_meters + 1)]
        all_batches = [meter_ids[i:i + batch_size] for i in range(0, len(meter_ids), batch_size)]

        # split into chunks of 1024
        chunks = [all_batches[i:i + max_per_expand] for i in range(0, len(all_batches), max_per_expand)]
        return chunks

    @task
    def process_chunk(chunk):
        from airflow.operators.python import get_current_context

        hostname = socket.gethostname()
        for meter_ids in chunk:
            logging.info(f"[{hostname}] Processing batch with {len(meter_ids)} meters. First meter: {meter_ids[0]}")
            hes_resp = requests.post(HES_URL, json={"meterIds": meter_ids}, timeout=30)
            hes_resp.raise_for_status()
            bulk_data = hes_resp.json().get("bulkReadings", [])
            payload = [{"meterId": m["meterId"], "readings": m["readings"]} for m in bulk_data]
            mdm_resp = requests.post(MDM_URL, json=payload, timeout=30)
            mdm_resp.raise_for_status()
            logging.info(f"[{hostname}] Done batch. MDM records sent: {len(payload)}")

    process_chunk.expand(chunk=generate_batches())

dag = hes_mdm_reduced_batches()
