from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

HES_URL = "http://hes-mock:3000/api/readings/bulk"
MDM_URL = "http://mdm-mock:4000/api/mdm/readings"

def generate_meter_batches(**context):
    meter_ids = [f"MTR{str(i).zfill(6)}" for i in range(1, 300001)]
    batches = [meter_ids[i:i + 100] for i in range(0, len(meter_ids), 100)]
    context['ti'].xcom_push(key='batches', value=batches)

def process_batch(meter_ids):
    # 1. Fetch from HES
    hes_response = requests.post(HES_URL, json={"meterIds": meter_ids})
    hes_response.raise_for_status()
    bulk_data = hes_response.json()["bulkReadings"]

    # 2. Flatten and send to MDM
    for meter in bulk_data:
        payload = {
            "meterId": meter["meterId"],
            "readings": meter["readings"]
        }
        mdm_response = requests.post(MDM_URL, json=payload)
        mdm_response.raise_for_status()

def create_child_tasks(dag, batches):
    from airflow.operators.python import PythonOperator

    for i, batch in enumerate(batches):
        PythonOperator(
            task_id=f"process_batch_{i}",
            python_callable=process_batch,
            op_args=[batch],
            dag=dag
        )

# Main DAG
with DAG(
    dag_id="hes_mdm_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["hes", "mdm"],
) as dag:

    gen_task = PythonOperator(
        task_id="generate_batches",
        python_callable=generate_meter_batches,
        provide_context=True
    )

    def dynamic_task_creator(**context):
        batches = context['ti'].xcom_pull(task_ids='generate_batches', key='batches')
        create_child_tasks(dag, batches)

    create_dynamic = PythonOperator(
        task_id="create_dynamic_tasks",
        python_callable=dynamic_task_creator,
        provide_context=True
    )

    gen_task >> create_dynamic
