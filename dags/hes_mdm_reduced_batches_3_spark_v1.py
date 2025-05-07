from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
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
def hes_mdm_reduced_batches_3_spark_v1():

    @task
    def generate_batches():
        meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, 300000)]
        batches = [meter_ids[i:i + 300] for i in range(0, len(meter_ids), 300)]
        logging.info(f"Generated {len(batches)} batches of 300 meters each.")
        return batches

    def create_spark_task(batch_id):
        return SparkKubernetesOperator(
            task_id=f'spark_pi_batch_{batch_id}',
            namespace='spark-apps',
            application_file='spark-pi.yaml',
            kubernetes_conn_id='kubernetes_default',
            do_xcom_push=False,
        )

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

    batches = generate_batches()
    for idx, batch in enumerate(batches.output):
        spark_task = create_spark_task(idx)
        spark_task >> process_batch_bulk.override(task_id=f'process_batch_{idx}')(batch)

dag = hes_mdm_reduced_batches_3_spark_v1()
