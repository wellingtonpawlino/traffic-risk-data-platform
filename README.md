
<h1 align="center">🚦 Plataforma de Dados de Risco de Trânsito </h1>

<p align="center">
  <img src="./assets/capa.png" alt="Banner Git Tutorial" width="600"/>
</p>




</p>





Arquitetura: **Bronze → Silver → Prep → Serving (Postgres) → BI (Superset)**  
Status: ✅ Pipeline End‑to‑End funcional  

---


# 📌 Visão Geral

Este projeto implementa uma **plataforma moderna de dados** para análise de risco de trânsito utilizando:

- Apache Airflow  
- MinIO (Data Lake)  
- PostgreSQL (Serving Layer)  
- Apache Superset (BI)  

O objetivo é construir um pipeline **reprodutível, escalável e analítico**, desde ingestão até visualização.

---

# 🧠 Contexto de Negócio

Simula um cenário do setor segurador:

Uma seguradora busca utilizar dados públicos para:

- 📈 Precificação de risco  
- 🛣️ Classificação de regiões e vias  
- ⚠️ Identificação de padrões de severidade  
- 💀 Análise de mortalidade no trânsito  

👉 O foco é o **risco do ambiente viário (contextual)**

---

# 🏗️ Arquitetura de Dados

---

## 🟫 Bronze (Raw)

- Dados ZIP originais do INFOSIGA  
- Sem transformação  
- Armazenados no MinIO  
- Particionamento por data (`dt=YYYY-MM-DD`)

---

## 🟪 Silver (Tratamento)

- Conversão para Parquet  
- Padronização de tipos  
- Manutenção da granularidade  

### Datasets:

- `sinistros`
- `pessoas`
- `veiculos`

---

## ⚙️ Prep Layer (Feature Engineering)

Camada criada neste projeto para enriquecer os dados com lógica de negócio.

### Enriquecimentos:

- número de pessoas por sinistro  
- número de veículos por sinistro  
- número de vítimas fatais  
- indicador de sinistro fatal  
- classificação de turno  

### 📊 tabela principal:


prep.sinistros_enriched

---

## 🟨 Serving Layer (PostgreSQL)

- Banco: `analytics`  
- Schema: `prep`  
- Tabela principal:

👉 utilizada diretamente no BI

---

## 📊 BI — Superset

- Conectado ao banco `analytics` ✅  
- Dataset criado ✅  
- Exploração via Explore ✅  

Capacidades:

- criação de gráficos  
- dashboards  
- SQL Lab  

---

# ⚙️ Pipeline de Dados

## 🔄 DAG principal

---

## 🎯 Etapas do pipeline

1. Leitura de dados da camada Silver (MinIO)  
2. Integração entre:
   - sinistros  
   - pessoas  
   - veículos  
3. Aplicação de feature engineering  
4. Escrita no Postgres  

---

## ✅ Execução Validada

- DAG executada com sucesso ✅  
- ~500k registros processados ✅  
- Escrita no Postgres validada ✅  
- Dados consumidos no Superset ✅  

---

# 🔧 Infraestrutura

Executada via Docker Compose:

- Airflow → orquestração  
- MinIO → data lake  
- PostgreSQL → armazenamento  
- Superset → BI  
- Jupyter → exploração  
- pgAdmin → administração  

---

# 🗄️ Governança de Dados

| Database | Função |
|--------|------|
| `airflow` | Metadados do Airflow |
| `superset` | Metadados do Superset |
| `analytics` | Dados analíticos |

---

# 🧠 Debugging e Aprendizados

Durante o desenvolvimento foram resolvidos problemas reais:

- ❌ erro de partição inexistente (`dt_ref`)  
- ❌ erro de escrita no Postgres (`duplicate type`)  
- ❌ erro de logs no Airflow  
- ❌ travamentos no consumo de dados  

✅ soluções aplicadas:

- fallback para última partição disponível  
- limpeza segura de tabelas antes da escrita  
- execução isolada fora do Airflow  
- validação direta via container  

---

# 🔄 Fluxo Git (Profissional)

- uso de feature branches ✅  
- commit semântico ✅  
- pull request ✅  
- merge na main ✅  
- limpeza de branches ✅  

---

# 📈 Exemplos de Análise

- acidentes por turno  
- fatalidades por turno  
- média de veículos por sinistro  

---

# 🚀 Próximos Passos

## Engenharia

- pipeline incremental  
- otimização de performance  
- particionamento no Postgres  

---

## Modelagem

- criação da camada Gold  
- star schema:
  - `fct_sinistros`
  - `dim_tempo`
  - `dim_municipio`

---

## BI

- dashboards analíticos completos  
- análise temporal  
- análise geográfica  

---

## Portfólio

- storytelling de negócio  
- documentação de insights  
- visual profissional  

---

# 🎯 Resultado

Este projeto demonstra:

✅ arquitetura moderna de dados  
✅ pipeline end-to-end  
✅ integração de múltiplas ferramentas  
✅ resolução de problemas reais  

---

# 🧠 Tecnologias

- Python  
- Apache Airflow  
- MinIO  
- PostgreSQL  
- Apache Superset  
- Docker  

---

# 👨‍💻 Wellington Santos

Projeto desenvolvido como prática avançada de Engenharia de Dados.

<h1 align="center">🚀 Projeto desenvolvido como prática avançada de Engenharia de Dados.</h1>

<p align="center">
  <img src="./assets/capa2.png" alt="Banner Git Tutorial" width="600"/>
</p>




</p>

---