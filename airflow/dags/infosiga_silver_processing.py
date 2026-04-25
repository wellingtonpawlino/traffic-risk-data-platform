from datetime import datetime
import os
import tempfile
import zipfile

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


# MinIO dentro da rede do docker-compose
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def list_bronze_objects(**context):
    ds = context["ds"]  # YYYY-MM-DD
    prefix = f"infosiga/dt={ds}/"

    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
    contents = resp.get("Contents", [])

    zips = [obj["Key"] for obj in contents if obj["Key"].endswith(".zip")]

    if len(zips) == 0:
        raise ValueError(f"Nenhum .zip encontrado em bronze://{BRONZE_BUCKET}/{prefix}")

    if len(zips) > 1:
        raise ValueError(f"Mais de um .zip encontrado em bronze://{BRONZE_BUCKET}/{prefix}: {zips}")

    return zips[0]


def extract_zip_from_bronze(ti, **context):
    zip_key = ti.xcom_pull(task_ids="list_bronze_objects")
    if not zip_key:
        raise ValueError("Não foi possível obter a key do ZIP da Bronze via XCom")

    s3 = _s3_client()

    tmp_dir = tempfile.mkdtemp(prefix="infosiga_silver_")
    local_zip_path = os.path.join(tmp_dir, "dados_infosiga.zip")

    # Baixa o ZIP do Bronze
    s3.download_file(BRONZE_BUCKET, zip_key, local_zip_path)

    extracted_paths = []
    with zipfile.ZipFile(local_zip_path, "r") as z:
        for name in z.namelist():
            if name.endswith("/") or not name.lower().endswith(".csv"):
                continue

            target_path = os.path.join(tmp_dir, os.path.basename(name))
            with z.open(name) as src, open(target_path, "wb") as dst:
                dst.write(src.read())

            extracted_paths.append(target_path)

    if len(extracted_paths) == 0:
        raise ValueError("ZIP extraído, mas nenhum CSV foi encontrado dentro dele.")

    # Retorna uma lista pequena de caminhos (metadados) via XCom (recomendado). [3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
    return extracted_paths


def write_parquet_to_silver(ti, **context):
    """
    Lê os CSVs extraídos (paths vindos da Task 2 via XCom),
    escreve Parquet local e envia para o MinIO bucket 'silver'.
    """
    ds = context["ds"]
    csv_paths = ti.xcom_pull(task_ids="extract_zip_from_bronze")
    if not csv_paths:
        raise ValueError("Não foi possível obter a lista de CSVs extraídos via XCom")

    s3 = _s3_client()

    # Converte cada CSV em um Parquet (um por arquivo), e sobe para silver
    for csv_path in csv_paths:
        base = os.path.basename(csv_path).replace(".csv", "")  # ex: pessoas_2015-2021
        # Leitura “simples” (sem transformações). Ajustes virão na Gold.
        df = pd.read_csv(csv_path,encoding="ISO-8859-1",sep=";",low_memory=False)


        # Escreve Parquet local (pandas suporta engine/ compressão etc.) [1](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_parquet.html)
        parquet_path = csv_path.replace(".csv", ".parquet")
        df.to_parquet(parquet_path, engine="pyarrow", index=False, compression="snappy")

        # Define key na Silver com partição por data e “tabela” por arquivo
        key = f"infosiga/{base}/dt={ds}/{base}.parquet"

        # Upload do arquivo local para S3/MinIO (upload_file). [2](https://boto3.amazonaws.com/v1/documentation/api/1.21.29/guide/s3-uploading-files.html)
        s3.upload_file(parquet_path, SILVER_BUCKET, key)

    return True


with DAG(
    dag_id="infosiga_silver_processing",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "infosiga"],
) as dag:

    list_bronze = PythonOperator(
        task_id="list_bronze_objects",
        python_callable=list_bronze_objects,
    )

    extract_zip = PythonOperator(
        task_id="extract_zip_from_bronze",
        python_callable=extract_zip_from_bronze,
    )

    write_silver = PythonOperator(
        task_id="write_parquet_to_silver",
        python_callable=write_parquet_to_silver,
    )

    list_bronze >> extract_zip >> write_silver