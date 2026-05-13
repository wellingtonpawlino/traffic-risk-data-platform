# airflow/dags/etl/infosiga_prep.py
from __future__ import annotations

import os
import re
import tempfile
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List

import boto3
import pandas as pd
from sqlalchemy import create_engine, text


# -----------------------------
# Config (padrões)
# -----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

POSTGRES_URI = os.getenv(
    "ANALYTICS_POSTGRES_URI",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics",
)

# Bases (ajuste caso mude o nome das pastas/arquivos)
SINISTROS_BASE = os.getenv("SINISTROS_BASE", "sinistros_2015-2021")
PESSOAS_BASE = os.getenv("PESSOAS_BASE", "pessoas_2015-2021")
VEICULOS_BASE = os.getenv("VEICULOS_BASE", "veiculos_2015-2021")


# -----------------------------
# Helpers
# -----------------------------
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def normalize_str(x: object) -> Optional[str]:
    """Normaliza texto: upper + remove acento + trim. Retorna None se vazio."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    s = s.upper()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


def to_int_safe(x) -> Optional[int]:
    try:
        if pd.isna(x):
            return None
        return int(float(x))
    except Exception:
        return None


def to_float_safe(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def parse_date_br(series: pd.Series) -> pd.Series:
    """Parse datas no formato dd/mm/yyyy para datetime64 (coerce)."""
    return pd.to_datetime(series, dayfirst=True, errors="coerce")


def parse_time_hhmm(series: pd.Series) -> pd.Series:
    """Parse hora HH:MM para datetime.time (coerce)."""
    dt = pd.to_datetime(series, format="%H:%M", errors="coerce")
    return dt.dt.time


def find_latest_dt(prefix_base: str) -> str:
    """
    Descobre o maior dt=YYYY-MM-DD existente no bucket SILVER para um base_prefix.
    Ex: prefix_base = 'infosiga/sinistros_2015-2021/'
    """
    s3 = s3_client()
    resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix_base)
    contents = resp.get("Contents", [])

    if not contents:
        raise ValueError(f"Nenhum objeto encontrado em s3://{SILVER_BUCKET}/{prefix_base}")

    dts = set()
    for obj in contents:
        key = obj["Key"]
        if "/dt=" in key:
            dt_part = key.split("/dt=")[1].split("/")[0]
            if len(dt_part) == 10:
                dts.add(dt_part)

    if not dts:
        raise ValueError(f"Nenhum dt=YYYY-MM-DD encontrado em s3://{SILVER_BUCKET}/{prefix_base}")

    return sorted(dts)[-1]


def download_parquet_from_s3(key: str) -> str:
    """Baixa um parquet do MinIO para um arquivo temporário e retorna o path local."""
    s3 = s3_client()
    tmp_dir = tempfile.mkdtemp(prefix="infosiga_prep_")
    local_path = os.path.join(tmp_dir, os.path.basename(key))
    s3.download_file(SILVER_BUCKET, key, local_path)
    return local_path


def load_silver_table(base_name: str, dt_ref: str) -> pd.DataFrame:
    """
    Carrega um parquet Silver no padrão:
    silver://infosiga/{base_name}/dt={dt_ref}/{base_name}.parquet
    """
    key = f"infosiga/{base_name}/dt={dt_ref}/{base_name}.parquet"
    local_path = download_parquet_from_s3(key)
    return pd.read_parquet(local_path, engine="pyarrow")


# -----------------------------
# Transformações (Silver -> Prepared)
# -----------------------------
def prep_sinistros(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # tipos básicos
    df["id_sinistro"] = df["id_sinistro"].apply(to_int_safe)
    df = df.dropna(subset=["id_sinistro"])
    df["id_sinistro"] = df["id_sinistro"].astype(int)

    # datas/horas
    if "data_sinistro" in df.columns:
        df["data_sinistro_dt"] = parse_date_br(df["data_sinistro"])
    if "hora_sinistro" in df.columns:
        df["hora_sinistro_t"] = parse_time_hhmm(df["hora_sinistro"])

    # normalizações de dimensões (turno/dia)
    for col in ["tipo_registro", "dia_da_semana", "turno", "ano_mes_sinistro"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_str)

    # flag fatal (para BI)
    if "tipo_registro" in df.columns:
        df["is_fatal_sinistro"] = df["tipo_registro"].eq("SINISTRO FATAL")

    # dedupe (1 linha por sinistro)
    df = df.drop_duplicates(subset=["id_sinistro"])

    # seleção mínima (mantém o resto se quiser, mas aqui já deixa mais leve)
    keep_cols = [c for c in df.columns]
    return df[keep_cols]


def prep_pessoas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ids
    df["id_sinistro"] = df["id_sinistro"].apply(to_int_safe)
    df = df.dropna(subset=["id_sinistro"])
    df["id_sinistro"] = df["id_sinistro"].astype(int)

    if "id_veiculo" in df.columns:
        df["id_veiculo"] = df["id_veiculo"].apply(to_int_safe)

    # textos importantes
    for col in ["municipio", "regiao_administrativa", "tipo_via", "tipo_veiculo_vitima", "sexo", "gravidade_lesao", "local_obito", "local_via"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_str)

    # cod_ibge / idade
    if "cod_ibge" in df.columns:
        df["cod_ibge"] = df["cod_ibge"].apply(to_int_safe)
    if "idade" in df.columns:
        df["idade"] = df["idade"].apply(to_float_safe)

    # datas
    if "data_obito" in df.columns:
        df["data_obito_dt"] = parse_date_br(df["data_obito"])

    # features úteis
    if "gravidade_lesao" in df.columns:
        df["is_fatal_pessoa"] = df["gravidade_lesao"].eq("FATAL")

    # faixa etária (BI)
    def faixa_etaria(idade):
        if idade is None or pd.isna(idade):
            return "NA"
        if idade < 18:
            return "MENOR_18"
        if idade < 30:
            return "18_29"
        if idade < 50:
            return "30_49"
        return "50+"

    if "idade" in df.columns:
        df["faixa_etaria"] = df["idade"].apply(faixa_etaria)

    # dedupe (linhas idênticas)
    df = df.drop_duplicates()

    return df


def prep_veiculos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["id_sinistro"] = df["id_sinistro"].apply(to_int_safe)
    df = df.dropna(subset=["id_sinistro"])
    df["id_sinistro"] = df["id_sinistro"].astype(int)

    if "id_veiculo" in df.columns:
        df["id_veiculo"] = df["id_veiculo"].apply(to_int_safe)

    for col in ["marca_modelo", "cor_veiculo", "tipo_veiculo", "ano_mes_sinistro"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_str)

    # datas
    if "data_sinistro" in df.columns:
        df["data_sinistro_dt"] = parse_date_br(df["data_sinistro"])

    df = df.drop_duplicates()
    return df


# -----------------------------
# Feature engineering (1 linha por sinistro para BI)
# -----------------------------
def build_sinistros_enriched(df_sin: pd.DataFrame, df_pes: pd.DataFrame, df_vei: pd.DataFrame) -> pd.DataFrame:
    """
    Gera uma tabela 1:1 por sinistro com contagens úteis para BI:
    - n_pessoas, n_fatais_pessoas
    - n_veiculos
    """
    # pessoas agregadas por sinistro
    pes_agg = (
        df_pes.groupby("id_sinistro", as_index=False)
              .agg(
                  n_pessoas=("id_sinistro", "size"),
                  n_fatais_pessoas=("is_fatal_pessoa", "sum") if "is_fatal_pessoa" in df_pes.columns else ("id_sinistro", "size"),
              )
    )
    if "is_fatal_pessoa" not in df_pes.columns:
        pes_agg["n_fatais_pessoas"] = 0

    # veiculos agregados por sinistro
    vei_agg = (
        df_vei.groupby("id_sinistro", as_index=False)
              .agg(n_veiculos=("id_sinistro", "size"))
    )

    # merge 1:1 no sinistro
    out = df_sin.merge(pes_agg, on="id_sinistro", how="left").merge(vei_agg, on="id_sinistro", how="left")
    out["n_pessoas"] = out["n_pessoas"].fillna(0).astype(int)
    out["n_fatais_pessoas"] = out["n_fatais_pessoas"].fillna(0).astype(int)
    out["n_veiculos"] = out["n_veiculos"].fillna(0).astype(int)

    # se a pessoa fatal existe, o sinistro pode ser tratado como fatal observado
    out["is_fatal_observado"] = out["n_fatais_pessoas"] > 0

    return out


# -----------------------------
# Load no Postgres (schema prep)
# -----------------------------
def write_to_postgres(df: pd.DataFrame, schema: str, table: str, if_exists: str = "replace") -> None:
    engine = create_engine(POSTGRES_URI)

    with engine.begin() as conn:
        # cria schema
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

        # quando for replace, limpa TABELA e TYPE órfão (Postgres cria type com nome da tabela)
        if if_exists == "replace":
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;'))
            conn.execute(text(f'DROP TYPE IF EXISTS "{schema}"."{table}" CASCADE;'))

    # agora cria de novo com pandas
    df.to_sql(
        table,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10_000,
    )
# -----------------------------
# Orquestração: Silver -> Prep -> Postgres
# -----------------------------
def run_preparation(dt_ref: Optional[str] = None) -> Dict[str, object]:
    """
    Executa toda a preparação em lote:
    - Lê sinistros/pessoas/veiculos da Silver (dt_ref)
    - Aplica transformações
    - Cria tabela enriquecida 1:1 por sinistro para BI
    - Persiste no Postgres (schema prep)
    """
    # descobre dt_ref automaticamente se não for informado
    if dt_ref is None:
        dt_ref = find_latest_dt(f"infosiga/{SINISTROS_BASE}/")

    # load
    df_sin = load_silver_table(SINISTROS_BASE, dt_ref)
    df_pes = load_silver_table(PESSOAS_BASE, dt_ref)
    df_vei = load_silver_table(VEICULOS_BASE, dt_ref)

    # prep
    sin_p = prep_sinistros(df_sin)
    pes_p = prep_pessoas(df_pes)
    vei_p = prep_veiculos(df_vei)

    # enriched (1 linha por sinistro)
    enriched = build_sinistros_enriched(sin_p, pes_p, vei_p)

    # persist
    write_to_postgres(sin_p, "prep", "sinistros", if_exists="replace")
    write_to_postgres(pes_p, "prep", "pessoas", if_exists="replace")
    write_to_postgres(vei_p, "prep", "veiculos", if_exists="replace")
    write_to_postgres(enriched, "prep", "sinistros_enriched", if_exists="replace")

    return {
        "dt_ref": dt_ref,
        "rows": {
            "prep.sinistros": int(sin_p.shape[0]),
            "prep.pessoas": int(pes_p.shape[0]),
            "prep.veiculos": int(vei_p.shape[0]),
            "prep.sinistros_enriched": int(enriched.shape[0]),
        }
    }
