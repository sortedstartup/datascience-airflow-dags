from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta
from pyspark.sql import SparkSession
import requests
import socket

# Define HES and MDM URLs
HES_URL = "http://hes-mock.mock/api/readings/bulk"
MDM_URL = "http://mdm-mock.mock/api/mdm/readings"

# Initialize Spark session
spark = SparkSession.builder.appName("MeterDataProcessing").getOrCreate()

# Input and Output paths
input_path = "/opt/airflow/data/meter_data.json"
output_path = "/opt/airflow/output/processed_data"

@dag(
    schedule_interval=None,  # Change as per your scheduling needs
    start_date=days_ago(1),
    catchup=False,
    tags=["hes", "mdm", "spark_jobs"],
)
def process_meter_data():

    @task
    def fetch_data_from_hes(meter_ids):
        """
        This task fetches the data from the HES system using a Spark job.
        """
        hostname = socket.gethostname()
        logging.info(f"[{hostname}] Fetching data for {len(meter_ids)} meters")

        # Make the request to HES API
        hes_resp = requests.post(HES_URL, json={"meterIds": meter_ids}, timeout=30)
        hes_resp.raise_for_status()
        bulk_data = hes_resp.json().get("bulkReadings", [])
        logging.info(f"[{hostname}] Received {len(bulk_data)} records from HES.")

        # Process data using Spark
        df = spark.createDataFrame(bulk_data)

        # Show the first few records (for debugging)
        df.show()

        # Save data to JSON for further processing
        df.write.json(input_path, mode='overwrite')

        return input_path  # Return the path where data is saved

    @task
    def process_and_dump_to_mdm(input_path):
        """
        This task processes the fetched data using a second Spark job and dumps it to the MDM system.
        """
        hostname = socket.gethostname()
        logging.info(f"[{hostname}] Processing data from {input_path} and dumping to MDM")

        # Read the data saved in the previous step
        df = spark.read.json(input_path)

        # Example transformation (optional)
        df.show()

        # Here you can apply any transformations or actions needed on the data
        # For example: df = df.filter(df['meterId'] == 'MTR000001')

        # Convert the DataFrame back to a list of dictionaries for the MDM API
        payload = [{"meterId": m["meterId"], "readings": m["readings"]} for m in df.collect()]

        # Send data to MDM API
        mdm_resp = requests.post(MDM_URL, json=payload, timeout=30)
        mdm_resp.raise_for_status()
        logging.info(f"[{hostname}] Sent {len(payload)} records to MDM.")

    @task
    def summarize_worker_usage(hostnames):
        """
        Summarize how many tasks were processed by each worker.
        """
        from collections import Counter
        count = Counter(hostnames)
        for worker, num_tasks in count.items():
            print(f"Worker '{worker}' ran {num_tasks} tasks")

    # Generate meter IDs (or you could load them from an external source)
    total_meters = 300000
    batch_size = 300
    meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, total_meters + 1)]
    
    # Split into batches for parallel processing (if needed)
    batches = [meter_ids[i:i + batch_size] for i in range(0, len(meter_ids), batch_size)]

    # Fetch data from HES (Spark job 1)
    input_path = fetch_data_from_hes.expand(meter_ids=batches)

    # Process data and dump to MDM (Spark job 2)
    process_and_dump_to_mdm(input_path)

    # Summarize worker usage (optional)
    summarize_worker_usage([socket.gethostname()] * len(batches))


# Instantiate the DAG
dag = process_meter_data()
