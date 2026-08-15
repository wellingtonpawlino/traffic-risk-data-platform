from datetime import datetime
import os
import tempfile
import zipfile

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


BUCKET = "traffic-risk-datalake-infosiga"
BRONZE_PREFIX = "bronze/infosiga"
SILVER_PREFIX = "silver/infosiga"


def _s3_client():
    return boto3.client("s3")


def list_bronze_objects(**context):
    ds = context["ds"]
    prefix = f"{BRONZE_PREFIX}/dt={ds}/"

    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    contents = resp.get("Contents", [])

    zips = [obj["Key"] for obj in contents if obj["Key"].endswith(".zip")]

    if len(zips) == 0:
        raise ValueError(f"Nenhum .zip encontrado em s3://{BUCKET}/{prefix}")

    if len(zips) > 1:
        raise ValueError(f"Mais de um .zip encontrado em s3://{BUCKET}/{prefix}: {zips}")

    return zips[0]


def extract_zip_from_bronze(ti, **context):
    zip_key = ti.xcom_pull(task_ids="list_bronze_objects")
    if not zip_key:
        raise ValueError("Nao foi possivel obter a key do ZIP da Bronze via XCom")

    s3 = _s3_client()

    tmp_dir = tempfile.mkdtemp(prefix="infosiga_silver_")
    local_zip_path = os.path.join(tmp_dir, "dados_infosiga.zip")

    s3.download_file(BUCKET, zip_key, local_zip_path)

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
        raise ValueError("ZIP extraido, mas nenhum CSV foi encontrado dentro dele.")

    return extracted_paths


def write_parquet_to_silver(ti, **context):
    ds = context["ds"]
    csv_paths = ti.xcom_pull(task_ids="extract_zip_from_bronze")
    if not csv_paths:
        raise ValueError("Nao foi possivel obter a lista de CSVs extraidos via XCom")

    s3 = _s3_client()

    for csv_path in csv_paths:
        base = os.path.basename(csv_path).replace(".csv", "")
        df = pd.read_csv(csv_path, encoding="ISO-8859-1", sep=";", low_memory=False)

        parquet_path = csv_path.replace(".csv", ".parquet")
        df.to_parquet(parquet_path, engine="pyarrow", index=False, compression="snappy")

        key = f"{SILVER_PREFIX}/{base}/dt={ds}/{base}.parquet"
        s3.upload_file(parquet_path, BUCKET, key)
        print(f"Enviado para s3://{BUCKET}/{key}")

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
