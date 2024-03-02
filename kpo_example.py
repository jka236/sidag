from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

resources_scala = k8s.V1ResourceRequirements(
    requests={"memory": "64Mi", "cpu": "250m"},
    limits={
        "memory": "64Mi",
        "cpu": 0.25,
    },
)

resources_python = k8s.V1ResourceRequirements(
    requests={"memory": "64Mi", "cpu": "250m", "ephemeral-storage": "1Gi"},
    limits={
        "memory": "128Mi",
        "cpu": 1,
    },
)

default_args = {
    "owner": "airflow",
}

with DAG(
    "spark_kubernetes_pod",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
) as dag:
    spark_scala = KubernetesPodOperator(
        task_id="pod-ex-minimum",
        name="pod-ex-minimum",
        namespace="airflow",
        image="spark:scala",
        cmds=["bash", "-cx"],
        arguments=["/opt/spark/bin/spark-shell --version"],
        resources=resources_scala,
    )

    spark_python = KubernetesPodOperator(
        task_id="pod-ex-minimum-2",
        name="pod-ex-minimum-2",
        namespace="airflow",
        image="spark:python3",
        cmds=["bash", "-cx"],
        arguments=["/opt/spark/bin/pyspark", "--version"],
        resources=resources_python,
    )

    spark_python >> spark_scala
