from datetime import datetime
import io
import os
import tempfile

import boto3
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = "traffic-risk-datalake-infosiga"
SILVER_PREFIX = "silver/infosiga"
RDS_HOST = os.environ["RDS_HOST"]
RDS_USER = "airflow"
RDS_PORT = 5432
CATEGORIES = ["pessoas", "sinistros", "veiculos"]


def _connect(dbname="analytics"):
    password = os.environ["DB_PASSWORD"]
    return psycopg2.connect(
        host=RDS_HOST, port=RDS_PORT, dbname=dbname,
        user=RDS_USER, password=password,
    )


def init_prep_schema(**context):
    conn = _connect(dbname="airflow")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'analytics'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE analytics")
        print("Database 'analytics' criado.")
    else:
        print("Database 'analytics' ja existe.")
    cur.close()
    conn.close()

    conn = _connect()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS prep")
    conn.commit()
    cur.close()
    conn.close()
    print("Schema 'prep' garantido.")


def load_category_to_prep(category, **context):
    ds = context["ds"]
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{SILVER_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if (
                len(parts) >= 4
                and parts[2].startswith(category)
                and f"dt={ds}" in parts[3]
                and key.endswith(".parquet")
            ):
                keys.append(key)

    if not keys:
        raise ValueError(
            f"Nenhum parquet encontrado para '{category}' em dt={ds} "
            f"no bucket s3://{BUCKET}/{SILVER_PREFIX}/"
        )

    print(f"{category}: {len(keys)} arquivos -> {keys}")

    tmp_dir = tempfile.mkdtemp(prefix=f"infosiga_prep_{category}_")
    conn = _connect()
    total_rows = 0

    for i, key in enumerate(sorted(keys)):
        local_path = os.path.join(tmp_dir, os.path.basename(key))
        s3.download_file(BUCKET, key, local_path)

        df = pd.read_parquet(local_path)
        # None/NaN → NULL no Postgres; strings vazias também viram NULL
        # (stg models tratam IS NULL e TRIM(col)='' da mesma forma)
        df = df.where(pd.notna(df), other=None)

        cur = conn.cursor()

        if i == 0:
            cols_ddl = ", ".join(f'"{c}" TEXT' for c in df.columns)
            cur.execute(f'DROP TABLE IF EXISTS prep."{category}"')
            cur.execute(f'CREATE TABLE prep."{category}" ({cols_ddl})')
            conn.commit()

        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="")
        buf.seek(0)

        cols_list = ", ".join(f'"{c}"' for c in df.columns)
        cur.copy_expert(
            f'COPY prep."{category}" ({cols_list}) FROM STDIN WITH CSV NULL \'\'',
            buf,
        )
        conn.commit()
        cur.close()

        total_rows += len(df)
        print(f"  {os.path.basename(key)}: {len(df)} linhas")

    conn.close()
    print(f"prep.{category}: {total_rows} linhas no total")


with DAG(
    dag_id="infosiga_silver_to_prep",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["prep", "infosiga"],
) as dag:

    init_schema = PythonOperator(
        task_id="init_prep_schema",
        python_callable=init_prep_schema,
    )

    load_tasks = [
        PythonOperator(
            task_id=f"load_{category}_to_prep",
            python_callable=load_category_to_prep,
            op_kwargs={"category": category},
        )
        for category in CATEGORIES
    ]

    init_schema >> load_tasks
