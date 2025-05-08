import yaml
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

NUM_JOBS = int(Variable.get("NUM_SPARK_JOBS", default_var=100))
TMP_DIR = "/opt/airflow/dags/tmp_specs"
os.makedirs(TMP_DIR, exist_ok=True)

default_args = {"start_date": days_ago(1)}

with DAG(
    dag_id="xask_spark_parallel_v2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    for idx in range(NUM_JOBS):
        job_name = f"spark-pi-{idx}"
        spec = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": job_name, "namespace": "spark-apps"},
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
                    "labels": {"version": "3.5.5"},
                },
                "executor": {
                    "cores": 1,
                    "instances": 2,
                    "memory": "512m",
                    "labels": {"version": "3.5.5"},
                },
            },
        }

        file_path = os.path.join(TMP_DIR, f"{job_name}.yaml")
        with open(file_path, "w") as f:
            yaml.dump(spec, f)

        SparkKubernetesOperator(
            task_id=f"spark_job_{idx}",
            namespace="spark-apps",
            application_file=file_path,
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
