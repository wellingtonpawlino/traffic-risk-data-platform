<h1 align="center">🚦 Traffic Risk Data Platform</h1>

<p align="center">
  <img src="./assets/capa.png" width="600"/>
</p>

<p align="center">
End-to-end data platform for traffic risk analysis using INFOSIGA public data
</p>

---

## 📌 Overview

Modern data platform for traffic risk analysis built on AWS, orchestrated with Apache Airflow, transformed with dbt, and visualized in Apache Superset.

The pipeline ingests raw accident data from INFOSIGA (São Paulo State traffic authority), processes it through a medallion architecture, and delivers analytical models ready for dashboards and risk scoring.

---

## 🧠 Business Context

Simulates a real insurance domain scenario where public traffic data is used to:

- 📈 Risk pricing by region and road type
- 🛣️ Road and municipality classification
- ⚠️ Severity and accident type pattern detection
- 💀 Traffic mortality analysis

---

## 🏗️ Architecture

```
INFOSIGA Portal
      │  (manual download)
      ▼
┌─────────────────────────────────────────┐
│           AWS S3 – Data Lake            │
│  bronze/infosiga/dt=YYYY-MM-DD/*.zip   │
│  silver/infosiga/{table}/dt=*/*.parquet│
└─────────────────────────────────────────┘
      │
      ▼  (psycopg2 COPY)
┌─────────────────────────────────────────┐
│        AWS RDS PostgreSQL               │
│  database: analytics                    │
│  ├── prep.*      (raw text tables)      │
│  ├── staging.*   (typed views – dbt)    │
│  └── marts.*     (facts & dims – dbt)   │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│         Apache Superset                 │
│  11 datasets · charts · dashboards      │
└─────────────────────────────────────────┘
```

---

## 🗂️ Data Layers

### 🟫 Bronze — Raw
- INFOSIGA ZIP files uploaded to S3
- No transformation, partitioned by `dt=YYYY-MM-DD`
- Path: `s3://traffic-risk-datalake-infosiga/bronze/infosiga/`

### 🟪 Silver — Parquet
- CSVs extracted from ZIP and converted to Parquet (Snappy)
- Encoding normalised (ISO-8859-1 → UTF-8), separator `;`
- 9 files across 3 domains × 3 time periods (2015-2021, 2022-2024, 2025-2026)
- Path: `s3://traffic-risk-datalake-infosiga/silver/infosiga/{table}/dt=YYYY-MM-DD/`

| Table | Rows |
|---|---|
| pessoas | 1,895,424 |
| sinistros | 1,407,814 |
| veiculos | 1,654,024 |

### 🟦 Prep — Serving (RDS)
- Silver Parquet loaded into PostgreSQL via `COPY` (bulk load)
- All columns stored as `TEXT` — type casting done downstream in dbt
- Schema: `prep` in database `analytics`

### 🟩 Marts — Analytical (dbt)
- **Staging** (`staging.*`): typed views with cleaned and cast columns
- **Marts** (`marts.*`): dimensional model

| Model | Type | Description |
|---|---|---|
| `fct_sinistros` | Table | 1.4M accident facts with metrics and flags |
| `fct_pessoas_sinistro` | Table | 1.9M person-accident facts |
| `dim_gravidade` | View | Injury severity dimension |
| `dim_local` | View | Location dimension (lat/lon/road) |
| `dim_local_pessoa` | View | Municipality and administrative region |
| `dim_pessoa` | View | Victim profile dimension |
| `dim_tipo_via` | View | Road type dimension |
| `dim_tipo_vitima` | View | Victim type dimension |
| `dim_tipo_sinistro` | View | Accident type dimension |
| `dim_tempo` | View | Date dimension |
| `dim_faixa_etaria` | View | Age group dimension |

---

## ⚙️ Airflow DAGs

| DAG | Trigger | Description |
|---|---|---|
| `infosiga_bronze_ingestion` | Manual | Uploads `dados_infosiga.zip` from `airflow/data/` to S3 bronze |
| `infosiga_silver_processing` | Manual | Extracts CSVs from bronze ZIP and converts to Parquet on S3 silver |
| `infosiga_silver_to_prep` | Manual | Loads silver Parquet into RDS `prep.*` via psycopg2 COPY |

> **Note:** INFOSIGA requires authenticated portal access. Download the ZIP manually and place it at `airflow/data/dados_infosiga.zip` before triggering `infosiga_bronze_ingestion`.

---

## 🛠️ Infrastructure

| Component | Technology | Details |
|---|---|---|
| Data Lake | AWS S3 | `traffic-risk-datalake-infosiga` (us-east-1) |
| Data Warehouse | AWS RDS PostgreSQL 15 | `analytics` database |
| Orchestration | Apache Airflow 2.9.0 | LocalExecutor, metadata on RDS |
| Transformation | dbt 1.10 + dbt-postgres | 14 models, custom schema macros |
| Visualisation | Apache Superset | Connected to RDS marts schema |
| Admin | pgAdmin 4 | Port 5050 |

---

## 🚀 Local Setup

### Prerequisites
- Docker + Docker Compose
- AWS credentials with S3 and RDS access
- `.env` file at project root

### `.env` file
```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
DB_PASSWORD=...
SUPERSET_SECRET_KEY=...
```

### Start services
```bash
docker compose up -d
```

Services:
- Airflow: http://localhost:8080 (`admin` / `admin`)
- Superset: http://localhost:8088 (`admin` / `admin`)
- pgAdmin: http://localhost:5050 (`admin@admin.com` / `admin`)

### Run the full pipeline
```bash
# 1. Place the INFOSIGA ZIP in airflow/data/
cp ~/Downloads/dados_infosiga.zip airflow/data/

# 2. Trigger DAGs in Airflow UI (in order):
#    infosiga_bronze_ingestion → infosiga_silver_processing → infosiga_silver_to_prep

# 3. Run dbt transformations
docker compose run --rm dbt dbt run
```

---

## 📁 Project Structure

```
traffic-risk-data-platform/
├── airflow/
│   ├── dags/
│   │   ├── infosiga_bronze_ingestion.py
│   │   ├── infosiga_silver_processing.py
│   │   └── infosiga_silver_to_prep.py
│   └── data/               # drop dados_infosiga.zip here
├── dbt/
│   ├── models/
│   │   ├── staging/        # stg_pessoas, stg_sinistros, stg_veiculos
│   │   └── marts/          # fct_* and dim_* models
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── profiles.yml        # gitignored – uses env_var('DB_PASSWORD')
├── superset/
│   └── superset_config.py
├── terraform/              # S3 bucket + RDS provisioning
└── docker-compose.yml
```

---

## 📊 Data Source

**INFOSIGA SP** — Sistema de Informações Gerenciais de Acidentes de Trânsito do Estado de São Paulo  
Portal: https://www.infosiga.sp.gov.br  
Coverage: 2015–2026 · Scope: São Paulo State · Granularity: per person, accident, and vehicle
