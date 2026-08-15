# Traffic Risk Data Platform

Pipeline de dados de sinistros de trânsito (INFOSIGA / DETRAN-SP), migrado de um setup
100% local (Docker Compose + MinIO) para um setup híbrido com armazenamento e banco na
AWS, mantendo orquestração e transformação self-hosted via Docker Compose. Projeto de
portfólio em engenharia de dados.

![capa](./assets/capa.png)

---

## Arquitetura

```
INFOSIGA (download manual — requer login gov.br / SSO)
  └─> S3 bronze   bronze/infosiga/dt=YYYY-MM-DD/dados_infosiga.zip
        └─> S3 silver   silver/infosiga/{tabela}_{periodo}/dt=YYYY-MM-DD/{tabela}.parquet
              └─> RDS PostgreSQL · database analytics · schema prep   (COPY nativo)
                    └─> dbt staging   analytics.staging.*   (views tipadas)
                          └─> dbt marts   analytics.marts.*   (star schema)
                                └─> Apache Superset   localhost:8088
```

Airflow orquestra as três DAGs (bronze → silver → prep).  
Todos os serviços de aplicação (Airflow, Superset, dbt, pgAdmin) rodam via Docker Compose
apontando para S3 e RDS reais na AWS. A máquina local não armazena dados persistentes.

---

## Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 2.9.0 (LocalExecutor) |
| Transformação | dbt 1.10 + dbt-postgres |
| Visualização | Apache Superset |
| Containers | Docker Compose |
| Infraestrutura como código | Terraform (AWS provider ~5.0) |
| Data Lake | AWS S3 (`traffic-risk-datalake-infosiga`, us-east-1) |
| Data Warehouse | AWS RDS PostgreSQL 15 (db.t3.micro, 20 GB) |
| Ingestão / carga | Python · boto3 · pandas · pyarrow · psycopg2 |

---

## Decisões de arquitetura

**Por que self-hosted em vez de MWAA ou Fargate?**
MWAA parte de ~USD 300/mês independente de uso; Fargate 24/7 para Airflow e Superset
acrescentaria outro custo fixo expressivo. RDS `db.t3.micro` cabe no free tier. Para um
projeto de portfólio que fica desligado entre demonstrações, a diferença é inviável.
LocalExecutor é suficiente para o volume e a frequência de execução deste pipeline — não
há paralelismo de workers que justifique CeleryExecutor aqui.

**Por que sem NAT Gateway?**
NAT Gateway cobra ~USD 35/mês de custo fixo, mais taxa por GB transferido. O RDS está em
subnet pública com `publicly_accessible = true`, mas o security group restringe o ingress
na porta 5432 a um único CIDR `/32` (o IP da máquina de desenvolvimento). Segurança
adequada para o caso de uso, sem custo fixo surpresa.

**Por que a ingestão do INFOSIGA é híbrida (manual + automatizado)?**
O portal oficial exige autenticação via SSO gov.br. Automatizar o login foi avaliado e
descartado: a sessão expira, o fluxo OAuth pode mudar sem aviso, e contornar isso cai em
práticas frágeis que não pertencem a um pipeline de produção. A automação cobre tudo a
partir do arquivo baixado — upload ao S3, extração de CSVs, conversão para Parquet e
carga no RDS.

**Por que COPY em vez de INSERT em lote na carga do silver para o RDS?**
Contra um banco remoto em us-east-1, cada batch de INSERT acumula latência de round-trip.
Com ~5 M de linhas, `psycopg2.cursor.copy_expert` (COPY nativo do PostgreSQL) envia os
dados como um stream contínuo para o servidor — sem overhead por lote, sem transações
intermediárias. Na prática: a mesma carga que levaria 30–60 minutos com `to_sql` e
`method="multi"` concluiu em ~6 minutos por tabela.

**Por que a infra sobe e desce sob demanda?**
`terraform apply` antes de demonstrar, `terraform destroy` ao terminar. O custo real do
projeto fora das sessões de uso é próximo de R$ 0/mês. Isso também força a infra a ser
completamente reproduzível via código — não há estado manual que precise ser preservado.

---

## DAGs

| DAG | Trigger | O que faz |
|---|---|---|
| `infosiga_bronze_ingestion` | Manual | Lê `dados_infosiga.zip` de `airflow/data/` e faz upload para `s3://traffic-risk-datalake-infosiga/bronze/infosiga/dt={ds}/` |
| `infosiga_silver_processing` | Manual | Baixa o ZIP do bronze, extrai CSVs (ISO-8859-1, separador `;`), converte para Parquet (Snappy) e sobe para o S3 silver particionado por data |
| `infosiga_silver_to_prep` | Manual | Baixa Parquets do silver e carrega via `COPY` nativo em `prep.pessoas`, `prep.sinistros`, `prep.veiculos` no RDS (3 tasks em paralelo) |

> O INFOSIGA exige login via gov.br. Baixe `dados_infosiga.zip` manualmente no portal e
> coloque em `airflow/data/` antes de disparar `infosiga_bronze_ingestion`.

---

## Modelos dbt

**Staging** (`analytics.staging.*`) — views que tipam e limpam colunas brutas do `prep`:  
`stg_pessoas` · `stg_sinistros` · `stg_veiculos`

**Marts** (`analytics.marts.*`) — star schema:

| Modelo | Tipo | Linhas |
|---|---|---|
| `fct_sinistros` | Table | 1.407.814 |
| `fct_pessoas_sinistro` | Table | 1.895.424 |
| `dim_gravidade` | View | — |
| `dim_local` | View | — |
| `dim_local_pessoa` | View | — |
| `dim_pessoa` | View | — |
| `dim_tipo_via` | View | — |
| `dim_tipo_vitima` | View | — |
| `dim_tipo_sinistro` | View | — |
| `dim_tempo` | View | — |
| `dim_faixa_etaria` | View | — |

---

## Como rodar

### Pré-requisitos

- Docker + Docker Compose
- Terraform CLI
- AWS CLI configurado com credenciais que têm acesso a S3 e RDS

### 1. Provisionar a infraestrutura

```bash
cd terraform
terraform init
terraform apply
```

Cria o bucket S3 `traffic-risk-datalake-infosiga` e o RDS `traffic-risk-postgres`
(db.t3.micro, PostgreSQL 15). Anote o endpoint do RDS no output.

### 2. Criar os databases no RDS

Esta etapa ainda não é gerenciada pelo Terraform. Conecte ao RDS via pgAdmin ou psql
(usando as credenciais do `terraform.tfvars`) e execute:

```sql
-- conectado ao database 'airflow' como usuário airflow
CREATE DATABASE analytics;
CREATE DATABASE superset;
```

O schema `prep` dentro de `analytics` é criado automaticamente pela DAG
`infosiga_silver_to_prep` na primeira execução (task `init_prep_schema`).

### 3. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Preencha o `.env`:

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
DB_PASSWORD=...           # senha definida no terraform.tfvars
SUPERSET_SECRET_KEY=...   # qualquer string longa e aleatória
```

### 4. Subir os serviços

```bash
docker compose up -d
```

| Serviço | URL | Credenciais padrão |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |

### 5. Baixar os dados

Acesse [infosiga.sp.gov.br](https://www.infosiga.sp.gov.br), faça login via gov.br,
baixe o arquivo de dados e salve em `airflow/data/dados_infosiga.zip`.

### 6. Executar o pipeline

Dispare as DAGs no Airflow UI em ordem, com a data de referência dos dados:

```
infosiga_bronze_ingestion → infosiga_silver_processing → infosiga_silver_to_prep
```

### 7. Rodar as transformações dbt

```bash
docker compose run --rm dbt dbt run
```

### 8. Acessar os dados no Superset

Acesse http://localhost:8088. Os 11 datasets de `marts.*` já estão registrados no banco
`Analytics RDS` e disponíveis para criar charts e dashboards.

### 9. Encerrar e destruir a infraestrutura

```bash
docker compose down
cd terraform && terraform destroy
```

O `terraform destroy` remove o RDS e o bucket S3, encerrando qualquer custo recorrente.

---

## Volume processado

~4,96 M de linhas distribuídas em três domínios, cobrindo o Estado de São Paulo de 2015
a 2026 com granularidade por pessoa, sinistro e veículo envolvido:

| Tabela | Linhas |
|---|---|
| `prep.pessoas` | 1.895.424 |
| `prep.sinistros` | 1.407.814 |
| `prep.veiculos` | 1.654.024 |

---

## Roadmap

- [x] Backend remoto para o state do Terraform (S3 com lock nativo — `use_lockfile = true`)
- [x] CI/CD com GitHub Actions: `terraform plan` em PR com comentário automático, `dbt parse` em push
- [ ] Testes dbt (`dbt test`) com asserções de unicidade e `not_null` nas PKs dos fatos
- [ ] Criação dos databases `analytics` e `superset` gerenciada pelo Terraform
- [x] Remover serviços legado do `docker-compose.yml` (MinIO, postgres local)

---

## Fonte dos dados

**INFOSIGA SP** — Sistema de Informações Gerenciais de Acidentes de Trânsito do Estado
de São Paulo  
Portal: https://www.infosiga.sp.gov.br
