from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

NUM_JOBS = int(Variable.get("NUM_SPARK_JOBS", default_var=100))

default_args = {
    "start_date": days_ago(1),
}

with DAG(
    dag_id="xask_spark_parallel",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "parallel"],
) as dag:

    for idx in range(NUM_JOBS):
        app_spec = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": f"spark-pi-{idx}",
                "namespace": "spark-apps",
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

        SparkKubernetesOperator(
            task_id=f"spark_job_batch_{idx}",
            namespace="spark-apps",
            application_file=app_spec,
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
