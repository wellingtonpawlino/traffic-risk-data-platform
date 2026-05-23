from datetime import datetime
import os
import tempfile

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
SILVER_BUCKET = "silver"

POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"

PESSOAS_DATASETS = [
    "pessoas_2015-2021",
    "pessoas_2022-2026"
]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def _list_all_parquets(s3, prefix):
    resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]


def _latest_parquet(s3, dataset):
    prefix = f"infosiga/{dataset}/dt="
    files = _list_all_parquets(s3, prefix)
    return sorted(files)[-1]


def load_pessoas(**context):
    s3 = _s3_client()
    dfs = []
    tmp_dir = tempfile.mkdtemp()

    for dataset in PESSOAS_DATASETS:
        key = _latest_parquet(s3, dataset)
        file = os.path.join(tmp_dir, dataset + ".parquet")

        print(f"📥 {key}")
        s3.download_file(SILVER_BUCKET, key, file)

        df = pd.read_parquet(file)
        print(f"✅ {dataset}: {df.shape}")

        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)
    print(f"🔥 Total pessoas: {df_final.shape}")

    engine = create_engine(POSTGRES_URI)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS prep;"))

    df_final.head(0).to_sql("pessoas", engine, schema="prep", if_exists="append", index=False)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE prep.pessoas;"))

    df_final.to_sql("pessoas", engine, schema="prep", if_exists="append", index=False, chunksize=50000)

    print("✅ prep.pessoas carregado")


with DAG(
    dag_id="infosiga_load_pessoas",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["load"],
) as dag:

    PythonOperator(
        task_id="load_pessoas",
        python_callable=load_pessoas
    )