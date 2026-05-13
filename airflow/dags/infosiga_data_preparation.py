# airflow/dags/infosiga_data_preparation.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.infosiga_prep import run_preparation

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="infosiga_data_preparation",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["prep", "silver", "gold"],
) as dag:

    prepare = PythonOperator(
        task_id="prepare_silver_for_gold",
        python_callable=lambda **ctx: run_preparation(dt_ref=None),
        provide_context=True,
    )

    prepare