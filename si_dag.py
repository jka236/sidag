from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
}

with DAG(
    "example_kubernetes_pod",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
) as dag:
    ubuntu16 = KubernetesPodOperator(
        task_id="pod-ex-minimum",
        name="pod-ex-minimum",
        namespace="default",
        image="spark:scala",
        cmds=["echo", "scala_version"],
    )

    ubuntu20 = KubernetesPodOperator(
        task_id="pod-ex-minimum-2",
        name="pod-ex-minimum-2",
        namespace="default",
        image="spark:python3",
        cmds=["echo", "python_version"],
    )

    ubuntu16 >> ubuntu20
