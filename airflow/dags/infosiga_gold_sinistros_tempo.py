from datetime import datetime
import os
import tempfile

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

# -----------------------------
# MinIO (dentro da rede do compose)
# -----------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
SILVER_BUCKET = "silver"

# -----------------------------
# Postgres (serving layer / gold)
# Aqui estou gravando no DB airflow por simplicidade operacional.
# Se você tiver um DB dedicado (ex: analytics), troque o database no final da URI.
# -----------------------------
POSTGRES_URI = os.getenv(
    "GOLD_POSTGRES_URI",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

# Dataset Silver (Key dentro do bucket "silver")
# Ajuste o nome/pasta se seu layout mudar
SINISTROS_BASE = "sinistros_2015-2021"
SINISTROS_KEY_TEMPLATE = f"infosiga/{SINISTROS_BASE}/dt={{ds}}/{SINISTROS_BASE}.parquet"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def build_gold_sinistros_tempo(**context):
    """
    Lê Parquet da camada Silver (MinIO) e cria uma agregação Gold:
    - total_sinistros por (ano_sinistro, mes_sinistro, turno)
    - total_fatais por (ano_sinistro, mes_sinistro, turno)
    Salva no Postgres em gold.sinistros_tempo
    """
    ds = context["ds"]
    s3 = _s3_client()

    key = SINISTROS_KEY_TEMPLATE.format(ds=ds)

    # Baixa o parquet para arquivo temporário (mais robusto que BytesIO com alguns engines)
    tmp_dir = tempfile.mkdtemp(prefix="gold_infosiga_")
    local_parquet = os.path.join(tmp_dir, f"{SINISTROS_BASE}_{ds}.parquet")

    s3.download_file(SILVER_BUCKET, key, local_parquet)

    df = pd.read_parquet(local_parquet, engine="pyarrow")

    # --- Transformação Gold (métrica de risco temporal) ---
    # Garantindo tipos mínimos
    # (se vier como string, converte)
    if df["ano_sinistro"].dtype == "object":
        df["ano_sinistro"] = pd.to_numeric(df["ano_sinistro"], errors="coerce")
    if df["mes_sinistro"].dtype == "object":
        df["mes_sinistro"] = pd.to_numeric(df["mes_sinistro"], errors="coerce")

    # fatal flag (ajuste se sua regra de fatalidade estiver em outra coluna)
    df["is_fatal"] = (df["tipo_registro"] == "SINISTRO FATAL")

    # agregação
    df_gold = (
        df.groupby(["ano_sinistro", "mes_sinistro", "turno"], dropna=False)
          .agg(
              total_sinistros=("id_sinistro", "nunique"),
              total_fatais=("is_fatal", "sum"),
          )
          .reset_index()
    )

    # carimbo de partição (opcional, ajuda governança/linhagem)
    df_gold["dt_ref"] = ds

    # --- Persistência no Postgres (serving) ---
    engine = create_engine(POSTGRES_URI)

    # cria schema gold se não existir
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))

    # escreve tabela
    df_gold.to_sql(
        name="sinistros_tempo",
        con=engine,
        schema="gold",
        if_exists="replace",  # depois podemos trocar para "append" com particionamento
        index=False,
        method="multi",
        chunksize=10_000,
    )

    # retorna algo pequeno (ok para logs)
    return {"rows": int(df_gold.shape[0]), "ds": ds, "table": "gold.sinistros_tempo"}


default_args = {"owner": "airflow"}

with DAG(
    dag_id="infosiga_gold_sinistros_tempo",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,   # manual como as outras
    catchup=False,
    tags=["gold", "infosiga"],
) as dag:

    build_and_load = PythonOperator(
        task_id="build_gold_sinistros_tempo",
        python_callable=build_gold_sinistros_tempo,
        provide_context=True,
    )

    build_and_load