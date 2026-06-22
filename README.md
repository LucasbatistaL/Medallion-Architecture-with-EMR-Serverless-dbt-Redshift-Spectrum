# Serverless Data Lakehouse na AWS

Arquitetura Medallion com EMR Serverless, dbt, Athena, Iceberg e Redshift Spectrum.

---

## Arquitetura

```
S3 (CSV bruto)
    |
EMR Serverless (PySpark) -> Bronze/Silver (Parquet no S3 + Glue Catalog)
    |
ECS Fargate (container dbt) -> Gold/Marts (Iceberg no S3 + Glue Catalog)
    |
Redshift Spectrum / Athena (camada de consumo)
```

Orquestrado por **AWS Step Functions** (EMR -> dbt em sequência).

---

## Stack

| Serviço | Função |
|---------|--------|
| AWS EMR Serverless | Processamento pesado (Bronze/Silver) com PySpark |
| dbt-core + dbt-athena | Transformações SQL (Silver para Gold) |
| AWS Athena | Engine SQL para as transformações do dbt |
| Apache Iceberg | Formato de tabela (time travel, schema evolution, ACID) |
| AWS S3 | Armazenamento do data lake |
| AWS Glue Catalog | Metastore de todas as tabelas |
| AWS ECS Fargate | Execução serverless do container dbt |
| AWS ECR | Registry de imagens Docker |
| AWS CodeBuild | CI/CD - build da imagem Docker na nuvem |
| AWS Step Functions | Orquestração do pipeline |
| AWS Redshift Spectrum | Engine de consulta para consumo |
| AWS CloudWatch | Logs de execução |

---

## Estrutura do Projeto

```
medallion-pipeline-aws/
├── dbt/
│   ├── Dockerfile
│   ├── buildspec.yml
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── .dockerignore
│   ├── .gitignore
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_fedex_shipments.sql      # Lê do seed (teste)
│   │   │   ├── stg_fedex_silver.sql         # Lê do output EMR (produção)
│   │   │   ├── schema.yml
│   │   │   └── sources.yml
│   │   ├── intermediate/
│   │   │   └── int_fedex_metricas.sql
│   │   └── marts/
│   │       └── mart_fedex_resumo_rota.sql   # Tabela Iceberg no S3
│   └── seeds/
│       ├── fedex.csv
│       └── fedex_shipments.csv
├── emr/
│   └── camada_silver.py
├── orchestration/
│   ├── step-functions-definition.json
│   └── README.md
├── iac/
│   └── task-definition.json
└── docs/
    └── como_funciona_dbt.md
```

---

## Fluxo de Dados (Arquitetura Medallion)

```
Bronze (EMR):  CSV -> filtra registros inválidos -> Parquet particionado por Year/Month
Silver (EMR):  + conversão timestamps, tempo viagem, route ID, validações, categorização atraso
Gold (dbt):    + agregações por rota, KPIs, tabelas Iceberg finais para consumo
```

### DAG (models dbt)

```
source: fedex_silver (Glue Catalog, vindo do EMR)
    |
staging: stg_fedex_silver (view) - renomeia colunas, padroniza
    |
intermediate: int_fedex_metricas (view) - regras de negócio, classificações
    |
marts: mart_fedex_resumo_rota (tabela Iceberg no S3) - KPIs agregados
    |
Redshift Spectrum / Athena / Dashboards
```

---

## Passo a Passo

### Pré-requisitos

- Conta AWS com acesso a: ECR, CodeBuild, ECS, EMR Serverless, Athena, S3, Glue, IAM, CloudWatch, Step Functions
- Repositório GitHub
- AWS CLI configurado (SSO recomendado)

---

### 1. Criar Bucket S3

```bash
aws s3api create-bucket --bucket [SEU_BUCKET] --region us-east-1
```

Subir dados e scripts:
```bash
aws s3 cp data/fedex.csv s3://[SEU_BUCKET]/data/fedex.csv
aws s3 cp emr/camada_silver.py s3://[SEU_BUCKET]/scripts/camada_silver.py
```

---

### 2. Criar Glue Database

Console: **Glue -> Databases -> Add database** -> Nome: `dbt_landing`

---

### 3. Criar Repositório ECR

Console: **ECR -> Create repository** -> Nome: `dbt-lakehouse`

---

### 4. Criar Projeto CodeBuild

- Source: GitHub (PAT classic com scope repo)
- Environment: EC2, Container, Ubuntu, Standard 7.0, Privileged marcado
- Variáveis: `AWS_ACCOUNT_ID`, `IMAGE_REPO_NAME`
- Após criar: attach `AmazonEC2ContainerRegistryPowerUser` na role

---

### 5. Criar Aplicação EMR Serverless

```bash
aws emr-serverless create-application --release-label emr-7.1.0 --type SPARK --name "medallion-silver"
```

---

### 6. Criar Cluster ECS

Console: **ECS -> Clusters -> Create** -> Fargate only -> Nome: `dbt-cluster`

---

### 7. Criar IAM Roles

**Task role** (para o container acessar serviços):
- Nome: `ecsTaskDbtRole`
- Trust: `ecs-tasks.amazonaws.com`
- Policies: `AmazonAthenaFullAccess`, `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess`

---

### 8. Criar Log Group

Console: **CloudWatch -> Log groups** -> Nome: `/ecs/dbt-validate`

---

### 9. Criar Task Definition

- Fargate, 0.25 vCPU, 0.5 GB
- Task role: `ecsTaskDbtRole`
- Entry point: `sh,-c`
- Command: `dbt seed && dbt run && dbt test`
- Logging: awslogs -> `/ecs/dbt-validate`

---

### 10. Criar Step Functions

Upload `orchestration/step-functions-definition.json` com seus valores.

---

### 11. Executar Pipeline

- **CodeBuild** -> Start build (cria imagem Docker)
- **Step Functions** -> Start execution (roda EMR depois dbt)

---

## Consumindo com Redshift Spectrum

```sql
CREATE EXTERNAL SCHEMA lakehouse
FROM DATA CATALOG
DATABASE 'dbt_landing'
IAM_ROLE 'arn:aws:iam::[ACCOUNT_ID]:role/[ROLE_REDSHIFT]'
REGION 'us-east-1';

SELECT * FROM lakehouse.mart_fedex_resumo_rota LIMIT 10;
```

---

## O que é gravado no S3

```
s3://[SEU_BUCKET]/
├── data/
│   └── fedex.csv                    <- Dado bruto
├── output/
│   └── fedex_silver/                <- Output do EMR (Parquet particionado)
│       ├── Year=2008/Month=1/
│       └── ...
├── tables/
│   └── dbt_landing/
│       └── mart_fedex_resumo_rota/  <- Output do dbt (Iceberg)
│           ├── metadata/            <- Metadados Iceberg (snapshots, schema)
│           └── data/                <- Arquivos Parquet com os dados
└── athena-staging/                  <- Resultados temporários do Athena
```

---

## Problemas Comuns

| Problema | Solução |
|----------|---------|
| Docker Hub 429 rate limit | Usar `public.ecr.aws/docker/library/python:3.11-slim` |
| Repo ECR não encontrado | Conferir variável `IMAGE_REPO_NAME` no CodeBuild |
| TYPE_MISMATCH nos testes | Adicionar `quote: false` no accepted_values |
| Conflito ENTRYPOINT | Colocar Entry point: `sh,-c` na task definition |
| Permissão negada (Athena/S3) | Attach policies de Athena/S3/Glue na task role |
| Database não existe | Usar `awsdatacatalog` pro Athena |
| Valores NA em colunas numéricas | Usar `try_cast()` no staging SQL |
| Git push rejeitado | `git pull origin main --rebase && git push` |
| Docker local sem virtualização | Não precisa Docker local - usar CodeBuild |

---

## Custos Estimados (por execução)

| Serviço | Custo |
|---------|-------|
| ECR | ~$0.10/GB/mês armazenamento |
| CodeBuild | ~$0.005/min |
| ECS Fargate | ~$0.01 por execução (0.25 vCPU, 5min) |
| EMR Serverless | ~$0.05 por execução (job pequeno) |
| Athena | $5/TB escaneado |
| S3 | $0.023/GB/mês |
| Step Functions | $0.025 por 1000 transições |

Total para este demo: **< $0.50 por execução completa do pipeline**.

---

## Licença

MIT
