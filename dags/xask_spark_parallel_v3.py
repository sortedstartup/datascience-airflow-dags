from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import copy

NUM_JOBS = int(Variable.get("NUM_SPARK_JOBS", default_var=100))

default_args = {"start_date": days_ago(1)}

base_spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "spark-pi",  # will be replaced dynamically
        "namespace": "spark-apps"
    },
    "spec": {
        "type": "Scala",
        "mode": "cluster",
        "image": "spark:3.5.5",
        "imagePullPolicy": "Always",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples.jar",
        "arguments": ["5000"],
        "sparkVersion": "3.5.5",
        "restartPolicy": {"type": "Never"},
        "driver": {
            "cores": 1,
            "memory": "512m",
            "labels": {"version": "3.5.5"}
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "512m",
            "labels": {"version": "3.5.5"}
        }
    }
}

with DAG(
    dag_id="xask_spark_parallel_v3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "inline"],
) as dag:

    for idx in range(NUM_JOBS):
        job_spec = copy.deepcopy(base_spec)
        job_spec["metadata"]["name"] = f"spark-pi-{idx}"

        SparkKubernetesOperator(
            task_id=f"spark_job_{idx}",
            namespace="spark-apps",
            application_file=job_spec,  # works with dict
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
