from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests
import logging

HES_URL = "http://hes-mock.mock/api/readings/bulk"
MDM_URL = "http://mdm-mock.mock/api/mdm/readings"

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["hes", "mdm"],
)
def hes_mdm_reduced_batches_3():

    @task
    def generate_batches():
        meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, 300001)]
        batches = [meter_ids[i:i + 300] for i in range(0, len(meter_ids), 300)]
        logging.info(f"Generated {len(batches)} batches of 300 meters each.")
        return batches

    @task
    def process_batch_bulk(meter_ids):
        logging.info(f"Processing batch with {len(meter_ids)} meters. First meter: {meter_ids[0]}")

        # 1. Bulk read from HES
        hes_resp = requests.post(HES_URL, json={"meterIds": meter_ids}, timeout=30)
        hes_resp.raise_for_status()
        bulk_data = hes_resp.json()["bulkReadings"]
        logging.info(f"Received readings for {len(bulk_data)} meters from HES.")

        # 2. Bulk write to MDM
        payload = [{"meterId": m["meterId"], "readings": m["readings"]} for m in bulk_data]
        mdm_resp = requests.post(MDM_URL, json=payload, timeout=30)
        mdm_resp.raise_for_status()
        logging.info(f"Successfully sent {len(payload)} records to MDM.")

    process_batch_bulk.expand(meter_ids=generate_batches())

dag = hes_mdm_reduced_batches_3()
