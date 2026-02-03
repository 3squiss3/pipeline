from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"retries": 2, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="demo_pipelines",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args=default_args,
    tags=["demo"],
) as dag:

    publish = BashOperator(
        task_id="pipeline2_publish",
        bash_command="python /opt/airflow/scripts/02_pipeline2_publish.py",
    )

    publish
