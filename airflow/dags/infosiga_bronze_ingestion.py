from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3

S3_BUCKET = "traffic-risk-datalake-infosiga"
S3_PREFIX = "bronze/infosiga"

LOCAL_FILE = "/opt/airflow/data/dados_infosiga.zip"


def upload_to_s3_bronze(**context):
    """Envia o ZIP baixado manualmente para o S3 (bronze)."""
    ds = context["ds"]

    if not os.path.exists(LOCAL_FILE):
        raise FileNotFoundError(
            f"Arquivo não encontrado: {LOCAL_FILE}\n"
            "Baixe dados_infosiga.zip do portal INFOSIGA e coloque em airflow/data/"
        )

    s3 = boto3.client("s3")
    key = f"{S3_PREFIX}/dt={ds}/dados_infosiga.zip"
    s3.upload_file(LOCAL_FILE, S3_BUCKET, key)
    print(f"Enviado para s3://{S3_BUCKET}/{key}")


default_args = {"owner": "airflow"}

with DAG(
    dag_id="infosiga_bronze_ingestion",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "infosiga"],
) as dag:

    upload_bronze = PythonOperator(
        task_id="upload_bronze",
        python_callable=upload_to_s3_bronze,
        provide_context=True,
    )
