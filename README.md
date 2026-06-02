<h1 align="center">🚦 Traffic Risk Data Platform</h1>

<p align="center">
  <img src="./assets/capa.png" width="600"/>
</p>

<p align="center">
End-to-end data platform for traffic risk analysis using Infosiga data
</p>

---

## 📌 Overview

This project implements a **modern data platform** for traffic risk analysis using:

- Apache Airflow  
- MinIO (Data Lake)  
- PostgreSQL (Serving Layer)  
- Apache Superset (BI)  

The goal is to build a **scalable, reproducible and analytical pipeline**, from ingestion to visualization.

---

## 🧠 Business Context

This project simulates a real scenario in the insurance domain:

An insurance company aims to leverage public data to:

- 📈 Risk pricing  
- 🛣️ Road and region classification  
- ⚠️ Severity pattern detection  
- 💀 Traffic mortality analysis  

👉 Focus: **contextual risk based on traffic conditions**

---

## 🏗️ Data Architecture

### 🟫 Bronze (Raw Layer)

- Original INFOSIGA ZIP files  
- No transformation  
- Stored in MinIO  
- Partitioned by date (`dt=YYYY-MM-DD`)  

---

### 🟪 Silver (Clean Layer)

- Converted to Parquet  
- Standardized data types  
- Maintained data granularity  

Datasets:
- `sinistros`
- `pessoas`
- `veiculos`

---

### ⚙️ Prep Layer (Feature Engineering)

Business logic applied to enrich datasets.

#### Enrichments:

- Number of people per accident  
- Number of vehicles per accident  
- Fatal victims count  
- Fatal accident indicator  
- Time-of-day classification  

#### Main table: