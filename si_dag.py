from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago

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
        arguments=["scala", "--version"],
    )

    spark_python = KubernetesPodOperator(
        task_id="pod-ex-minimum-2",
        name="pod-ex-minimum-2",
        namespace="airflow",
        image="spark:python3",
        cmds=["bash", "-cx"],
        arguments=["python", "--version"],
    )

    spark_python >> spark_scala
