from datetime import datetime
import os
import tempfile

import boto3
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator

# -----------------------------
# MinIO
# -----------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
SILVER_BUCKET = "silver"

# -----------------------------
# Postgres (DW)
# -----------------------------
POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"

# -----------------------------
# Config datasets
# -----------------------------
SINISTROS_DATASETS = [
    "sinistros_2021-2021",
    "sinistros_2022-2026"
]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def load_sinistros_to_postgres(**context):
    s3 = _s3_client()
    ds = context["ds"]

    tmp_dir = tempfile.mkdtemp()
    dfs = []

    for dataset in SINISTROS_DATASETS:
        key = f"infosiga/{dataset}/dt={ds}/{dataset}.parquet"
        local_file = os.path.join(tmp_dir, f"{dataset}.parquet")

        print(f"📥 Baixando: {key}")
        s3.download_file(SILVER_BUCKET, key, local_file)

        df = pd.read_parquet(local_file)
        print(f"✅ {dataset} -> {df.shape}")

        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    print(f"🔥 Total final: {df_final.shape}")

    engine = create_engine(POSTGRES_URI)

    df_final.to_sql(
        "silver_sinistros_raw",
        con=engine,
        schema="staging",
        if_exists="replace",
        index=False,
    )

    print("✅ Dados de sinistros carregados no Postgres")


default_args = {"owner": "airflow"}

with DAG(
    dag_id="infosiga_load_sinistros",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["load", "postgres", "sinistros"],
) as dag:

    load_task = PythonOperator(
        task_id="load_sinistros",
        python_callable=load_sinistros_to_postgres,
        provide_context=True,
    )

    load_task