from datetime import datetime
import os
import tempfile

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

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
# Postgres
# -----------------------------
POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"

# -----------------------------
# Datasets
# -----------------------------
SINISTROS_DATASETS = [
    "sinistros_2015-2021",
    "sinistros_2022-2026"
]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def _list_all_parquets(s3, bucket: str, prefix: str) -> list[str]:
    keys = []
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet"):
                keys.append(k)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys


def _latest_parquet_key_for_dataset(s3, dataset: str) -> str:
    prefix = f"infosiga/{dataset}/dt="
    keys = _list_all_parquets(s3, SILVER_BUCKET, prefix)

    if not keys:
        raise ValueError(f"Nenhum parquet encontrado para {dataset}")

    return sorted(keys)[-1]


def load_sinistros_to_postgres(**context):
    s3 = _s3_client()
    tmp_dir = tempfile.mkdtemp()
    dfs = []

    # opcional: permitir override manual
    dt_ref = None
    if context.get("dag_run") and context["dag_run"].conf:
        dt_ref = context["dag_run"].conf.get("dt_ref")

    for dataset in SINISTROS_DATASETS:
        if dt_ref:
            key = f"infosiga/{dataset}/dt={dt_ref}/{dataset}.parquet"
        else:
            key = _latest_parquet_key_for_dataset(s3, dataset)

        local_file = os.path.join(tmp_dir, f"{dataset}.parquet")

        print(f"📥 Baixando: {key}")
        s3.download_file(SILVER_BUCKET, key, local_file)

        df = pd.read_parquet(local_file)
        print(f"✅ {dataset} -> {df.shape}")

        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)
    print(f"🔥 Total final: {df_final.shape}")

    engine = create_engine(POSTGRES_URI)

    # -----------------------------
    # ✅ CRIA SCHEMA SE NÃO EXISTIR
    # -----------------------------
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS prep;"))

    # -----------------------------
    # ✅ GARANTE TABELA (SEM DROPAR)
    # -----------------------------
    df_final.head(0).to_sql(
        name="sinistros",
        con=engine,
        schema="prep",
        if_exists="append",
        index=False
    )

    # -----------------------------
    # ✅ LIMPA DADOS (SEM QUEBRAR DBT)
    # -----------------------------
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE prep.sinistros;"))

    # -----------------------------
    # ✅ INSERE DADOS
    # -----------------------------
    df_final.to_sql(
        name="sinistros",
        con=engine,
        schema="prep",
        if_exists="append",
        index=False,
        chunksize=50000
    )

    print("✅ Dados carregados em prep.sinistros")


default_args = {"owner": "airflow"}

with DAG(
    dag_id="infosiga_load_sinistros",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["load", "postgres"],
) as dag:

    load_task = PythonOperator(
        task_id="load_sinistros",
        python_callable=load_sinistros_to_postgres,
    )

    load_task