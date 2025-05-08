from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import copy

NUM_JOBS = int(Variable.get("NUM_SPARK_JOBS", default_var=100))

default_args = {"start_date": days_ago(1)}

with DAG(
    dag_id="xask_spark_parallel_v5",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "inline"],
) as dag:

    for idx in range(NUM_JOBS):
        SparkKubernetesOperator(
            task_id=f"spark_job_{idx}",
            namespace="default",
            application_file="spark-pi.yaml",  # <- comma added here
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
