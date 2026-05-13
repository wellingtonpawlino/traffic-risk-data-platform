from datetime import datetime
import os
import tempfile

import boto3
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator


# MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
SILVER_BUCKET = "silver"

# Postgres
POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def load_silver_to_postgres():
    s3 = _s3_client()

    # 🔴 AJUSTE AQUI (use o dt que você sabe que existe)
    key = "infosiga/sinistros_2015-2021/dt=2026-04-25/sinistros_2015-2021.parquet"

    tmp_dir = tempfile.mkdtemp()
    local_file = os.path.join(tmp_dir, "sinistros.parquet")

    # baixar do MinIO
    s3.download_file(SILVER_BUCKET, key, local_file)

    # ler parquet
    df = pd.read_parquet(local_file)

    print(f"Linhas carregadas: {df.shape}")

    # conectar Postgres
    engine = create_engine(POSTGRES_URI)

    # salvar tabela
    df.to_sql(
        "silver_sinistros_raw",
        con=engine,
        if_exists="replace",
        index=False
    )

    print("✅ dados carregados no Postgres")


default_args = {"owner": "airflow"}

with DAG(
    dag_id="infosiga_gold_simple_load",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "test"],
) as dag:

    load_task = PythonOperator(
        task_id="load_silver_to_postgres",
        python_callable=load_silver_to_postgres,
    )

    load_task