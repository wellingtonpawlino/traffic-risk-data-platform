# 🚦 Plataforma de Dados de Risco de Trânsito — INFOSIGA

Arquitetura: **Bronze → Silver → Gold**  
Status: ✅ Infra concluída | ✅ Camada de Serving implementada | 🚧 Data Understanding em andamento  

---

# 📌 Visão Geral

Este projeto implementa uma **plataforma analítica de dados de risco de trânsito** utilizando Docker, Airflow, MinIO, Postgres, Superset e Jupyter, seguindo a arquitetura de dados em camadas (Bronze / Silver / Gold).

O objetivo é construir uma base analítica confiável e reprodutível para análise de:
- risco viário  
- severidade de sinistros  
- mortalidade no trânsito  

Utilizando os dados públicos do **INFOSIGA-SP (DETRAN-SP)**.

---

# 🧠 Contexto de Negócio

O projeto simula um cenário real do setor segurador:

Uma seguradora automotiva deseja utilizar dados públicos para melhorar:
- 📈 Precificação de risco  
- 🛣️ Classificação de vias e regiões  
- ⚠️ Identificação de padrões de severidade  
- 🤝 Parcerias com o poder público  

👉 O foco é o **risco contextual do ambiente viário**, não o perfil individual.

---

# 🏗️ Arquitetura
## 🔸 Visão geral do fluxo

---

# ⚙️ Infraestrutura

Toda a stack roda via **Docker Compose**.

## 🧩 Serviços

- **Airflow** → Orquestração de pipelines  
- **MinIO** → Data Lake (Bronze / Silver)  
- **Postgres** → Serving Layer (Gold)  
- **Superset** → Visualização / BI  
- **Jupyter** → Data Understanding  
- **pgAdmin** → Gestão do banco  

---

## ✅ Validações realizadas

- Conexão Jupyter ↔ MinIO ✅  
- Buckets (`bronze`, `silver`, `gold`) ✅  
- Leitura de Parquet ✅  
- DAGs do Airflow executando ✅  
- Escrita no Postgres ✅  
- Superset conectado ao banco ✅  
- Dataset criado e consultado ✅  

---

# 🗄️ Governança de Dados

Separação de responsabilidades no Postgres:

| Database | Função |
|--------|------|
| `airflow` | Metadados do Airflow |
| `superset` | Metadados do Superset |
| `analytics` | Dados analíticos (Gold) |

👉 Evita ambiente poluído e melhora governança.

---

# 📂 Camadas de Dados

## 🟫 Bronze

- Dados ZIP originais do INFOSIGA  
- Sem transformação  
- Armazenados no MinIO  
- Particionados por data (`dt=YYYY-MM-DD`)

---

## 🟪 Silver

- Dados convertidos para **Parquet**
- Tipos padronizados
- Granularidade preservada
- Sem agregações

### Datasets:
- `sinistros`
- `pessoas`
- `veiculos`

### Modelo lógico:

public.silver_sinistros_raw

---

## 🔄 DAGs implementadas

### 1. `infosiga_gold_simple_load`
- Lê Parquet da Silver  
- Carrega no Postgres  
- Usado para validação do pipeline  

---

### 2. `infosiga_gold_sinistros_tempo`
- Base para agregações analíticas  
- Evolução futura da camada Gold  

---

# 📊 BI — Superset

## ✅ Configurado

- Conexão com banco `analytics`
- Dataset criado:


silver_sinistros_raw

---

## Capabilidades

- Exploração de dados  
- SQL Lab  
- Criação de gráficos  
- Base para dashboards  

---

# 🚀 Status Atual

| Etapa | Status |
|------|------|
| Infraestrutura | ✅ |
| Ingestão (Bronze) | ✅ |
| Processamento (Silver) | ✅ |
| Serving (Gold) | ✅ |
| BI (Superset) | ✅ |
| Insights | 🚧 |

---

# 📌 Próximos Passos

## 🎯 Engenharia

- Evoluir DAGs para processamento incremental  
- Criar schema dedicado `gold.*`  
- Aprimorar pipeline Silver → Gold  

---

## 📊 Análise

- Criar métricas de risco:
  - taxa de mortalidade  
  - severidade de acidentes  
  - risco por município  
- Agregações analíticas  

---

## 📈 BI / Dashboards

- Dashboard de risco temporal  
- Análise por turno  
- Distribuição por tipo de sinistro  
- Visualização geográfica  

---

## 💼 Portfólio

- Adicionar storytelling de negócio  
- Documentar insights reais  
- Demonstrar pipeline end-to-end  

---

# 🎯 Resultado

Esta plataforma representa uma:

✅ Arquitetura moderna de dados  
✅ Pipeline completo de ingestão → consumo  
✅ Separação clara de camadas  
✅ Integração com BI  

---

# 🧠 Tecnologias

- Python  
- Apache Airflow  
- MinIO  
- PostgreSQL  
- Apache Superset  
- Docker  

---

# 👨‍💻 Autor

Projeto desenvolvido como prática avançada de Engenharia de Dados.


🔥 Resultado
Esse README agora está:
✅ Profissional
✅ Claro para recrutador
✅ Estruturado como projeto de empresa
✅ Mostrando arquitetura + negócio