from datetime import datetime
import os
import tempfile
import gc

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

SINISTROS_DATASETS = [
    "sinistros_2015-2021",
    "sinistros_2022-2026",
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
    if not files:
        raise ValueError(f"Nenhum parquet encontrado para {dataset}")
    return sorted(files)[-1]

def load_sinistros(**context):
    s3 = _s3_client()
    tmp_dir = tempfile.mkdtemp()

    engine = create_engine(POSTGRES_URI)

    # 1) schema prep
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS prep;"))

    # 2) cria tabela (sem drop) usando um DF vazio baseado no primeiro dataset
    first_key = _latest_parquet(s3, SINISTROS_DATASETS[0])
    first_file = os.path.join(tmp_dir, "bootstrap.parquet")
    print(f"📥 bootstrap: {first_key}")
    s3.download_file(SILVER_BUCKET, first_key, first_file)
    df0 = pd.read_parquet(first_file)
    df0.head(0).to_sql("sinistros", engine, schema="prep", if_exists="append", index=False)
    del df0
    gc.collect()

    # 3) overwrite lógico (sem DROP): TRUNCATE
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE prep.sinistros;"))

    # 4) carrega dataset por dataset (sem concat)
    for dataset in SINISTROS_DATASETS:
        key = _latest_parquet(s3, dataset)
        local_path = os.path.join(tmp_dir, f"{dataset}.parquet")

        print(f"📥 {key}")
        s3.download_file(SILVER_BUCKET, key, local_path)

        df = pd.read_parquet(local_path)
        print(f"✅ {dataset}: {df.shape}")

        df.to_sql(
            "sinistros",
            engine,
            schema="prep",
            if_exists="append",
            index=False,
            chunksize=50_000,
        )

        # libera memória agressivamente
        del df
        gc.collect()

    print("✅ prep.sinistros carregado (streaming, sem concat)")

with DAG(
    dag_id="infosiga_load_sinistros",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["load"],
) as dag:
    PythonOperator(
        task_id="load_sinistros",
        python_callable=load_sinistros,
    )