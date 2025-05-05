from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests

HES_URL = "http://hes-mock:3000/api/readings/bulk"
MDM_URL = "http://mdm-mock:4000/api/mdm/readings/bulk"

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["hes", "mdm"],
)
def hes_mdm_reduced_batches():

    @task
    def generate_batches():
        meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, 300001)]
        # Batching into 300-meter groups => 1000 batches
        return [meter_ids[i:i + 300] for i in range(0, len(meter_ids), 300)]

    @task
    def process_batch_bulk(meter_ids):
        # 1. Bulk read from HES
        hes_resp = requests.post(HES_URL, json={"meterIds": meter_ids}, timeout=30)
        hes_resp.raise_for_status()
        bulk_data = hes_resp.json()["bulkReadings"]

        # 2. Bulk write to MDM
        payload = [{"meterId": m["meterId"], "readings": m["readings"]} for m in bulk_data]
        mdm_resp = requests.post(MDM_URL, json=payload, timeout=30)
        mdm_resp.raise_for_status()

    process_batch_bulk.expand(meter_ids=generate_batches())

dag = hes_mdm_reduced_batches()
