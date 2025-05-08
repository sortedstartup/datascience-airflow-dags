from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="spark_pi_example",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "example"],
) as dag:

    run_spark_pi = SparkKubernetesOperator(
        task_id="run_spark_pi",
        namespace="spark-apps",  # Must match SparkApplication namespace
        application_file="spark-pi.yaml",  # Adjust if placed elsewhere
        kubernetes_conn_id="kubernetes_default",  # Ensure this points to right cluster
        do_xcom_push=False,
    )
