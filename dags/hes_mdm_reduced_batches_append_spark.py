from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import logging
import requests

SPARK_CONTAINER_URL = "http://spark-job-container:5000/process_batch"  # Replace with actual Spark container URL

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["hes", "mdm", "append_spark"],
)
def hes_mdm_reduced_batches_append_spark():

    @task
    def generate_batches():
        total_meters = 300000
        batch_size = 300

        # Generate 1000 batches of 300 meters each
        meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, total_meters + 1)]
        batches = [meter_ids[i:i + batch_size] for i in range(0, len(meter_ids), batch_size)]
        logging.info(f"Generated {len(batches)} batches of {batch_size} meters each.")
        return batches[:1000]  # Ensure exactly 1000 batches

    @task
    def trigger_spark_job(meter_ids):
        batch_name = f"Batch_{meter_ids[0]}"  # Create a batch name from the first meter ID
        logging.info(f"Triggering Spark job for {batch_name}")

        # Send batch data to Spark container for processing
        response = requests.post(SPARK_CONTAINER_URL, json={"batch_name": batch_name})

        if response.status_code == 200:
            logging.info(f"Spark job triggered successfully for {batch_name}")
        else:
            logging.error(f"Failed to trigger Spark job for {batch_name}")

    # Generate batches and trigger Spark jobs for each batch
    batches = generate_batches()
    trigger_spark_job.expand(meter_ids=batches)

# Instantiate the DAG
dag = hes_mdm_reduced_batches_append_spark()
