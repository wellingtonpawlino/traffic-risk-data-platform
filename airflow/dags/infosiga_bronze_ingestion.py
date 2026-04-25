from datetime import datetime
import os
import tempfile
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3


INFOSIGA_ZIP_URL = "https://infosiga.detran.sp.gov.br/rest/painel/download/file/dados_infosiga.zip"
# URL publicada no portal de dados abertos de SP para o recurso ZIP do Infosiga 

MINIO_ENDPOINT = "http://minio:9000"  # dentro da rede do docker-compose
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
BRONZE_BUCKET = "bronze"


def download_infosiga_zip(**context):
    """Baixa o ZIP do INFOSIGA para um arquivo temporário local (dentro do container)."""
    ds = context["ds"]  # YYYY-MM-DD
    tmp_dir = tempfile.mkdtemp(prefix="infosiga_")
    local_path = os.path.join(tmp_dir, f"dados_infosiga_{ds}.zip")

    with requests.get(INFOSIGA_ZIP_URL, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

  
    return local_path


def upload_to_minio_bronze(ti, **context):
    """Envia o ZIP para o MinIO (bucket bronze) sem transformar nada."""
    ds = context["ds"]
    local_path = ti.xcom_pull(task_ids="download_zip")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    # Particionamento simples por data de execução (Bronze)
    key = f"infosiga/dt={ds}/dados_infosiga.zip"

    s3.upload_file(local_path, BRONZE_BUCKET, key)


default_args = {"owner": "airflow"}

with DAG(
    dag_id="infosiga_bronze_ingestion",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  
    catchup=False,
    tags=["bronze", "infosiga"],
) as dag:

    download_zip = PythonOperator(
        task_id="download_zip",
        python_callable=download_infosiga_zip,
        provide_context=True,
    )

    upload_bronze = PythonOperator(
        task_id="upload_bronze",
        python_callable=upload_to_minio_bronze,
        provide_context=True,
    )

    download_zip >> upload_bronze