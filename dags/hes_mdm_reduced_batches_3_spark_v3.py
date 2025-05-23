from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import requests
import logging

HES_URL = "http://hes-mock.mock/api/readings/bulk"
MDM_URL = "http://mdm-mock.mock/api/mdm/readings"

@task
def process_batch_bulk(meter_ids, batch_id):
    logging.info(f"[Batch {batch_id}] Processing {len(meter_ids)} meters. First: {meter_ids[0]}")
    hes_resp = requests.post(HES_URL, json={"meterIds": meter_ids}, timeout=30)
    hes_resp.raise_for_status()
    bulk_data = hes_resp.json()["bulkReadings"]
    payload = [{"meterId": m["meterId"], "readings": m["readings"]} for m in bulk_data]
    mdm_resp = requests.post(MDM_URL, json=payload, timeout=30)
    mdm_resp.raise_for_status()
    logging.info(f"[Batch {batch_id}] Sent {len(payload)} records to MDM.")

@dag(schedule_interval=None, start_date=days_ago(1), catchup=False, tags=["hes", "mdm"])
def hes_mdm_reduced_batches_3_spark_v3():
    pass  # task definitions will be attached outside

dag = hes_mdm_reduced_batches_3_spark_v3()

# Define batch data outside DAG body
meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, 300001)]
batches = [meter_ids[i:i + 300] for i in range(0, len(meter_ids), 300)]

# Add fixed number of batch tasks
for idx, batch in enumerate(batches[:5]):  # limit to 5 for now
    spark = SparkKubernetesOperator(
        task_id=f"spark_job_batch_{idx}",
        namespace="default",
        application_file="spark-pi.yaml",  # optionally templated per batch
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        dag=dag,
    )

    post_spark = process_batch_bulk.override(task_id=f"process_batch_{idx}").expand_kwargs(
        [{"meter_ids": batch, "batch_id": idx}]
    )
    spark >> post_spark
