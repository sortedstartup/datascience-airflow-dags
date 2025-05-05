from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests

HES_URL = "http://hes-mock:3000/api/readings/bulk"
MDM_URL = "http://mdm-mock:4000/api/mdm/readings/bulk"  # Assuming MDM bulk endpoint

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["hes", "mdm"],
)
def hes_mdm_new_pipeline():

    @task
    def generate_batches():
        meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, 300001)]
        return [meter_ids[i:i + 100] for i in range(0, len(meter_ids), 100)]

    @task
    def process_batch_bulk(meter_ids):
        # 1. Fetch from HES
        hes_resp = requests.post(HES_URL, json={"meterIds": meter_ids}, timeout=30)
        hes_resp.raise_for_status()
        bulk_data = hes_resp.json()["bulkReadings"]

        # 2. Submit in one MDM bulk request
        mdm_payload = [
            {"meterId": meter["meterId"], "readings": meter["readings"]}
            for meter in bulk_data
        ]

        mdm_resp = requests.post(MDM_URL, json=mdm_payload, timeout=30)
        mdm_resp.raise_for_status()

    # Chain the tasks
    process_batch_bulk.expand(meter_ids=generate_batches())

dag = hes_mdm_pipeline()
