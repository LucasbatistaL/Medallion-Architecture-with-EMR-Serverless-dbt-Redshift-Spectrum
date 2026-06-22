# Serverless Data Lakehouse on AWS

Medallion Architecture with EMR Serverless, dbt, Athena, Iceberg & Redshift Spectrum.

---

## Architecture

```
S3 (CSV raw data)
    |
EMR Serverless (PySpark) -> Bronze/Silver (Parquet on S3 + Glue Catalog)
    |
ECS Fargate (dbt container) -> Gold/Marts (Iceberg on S3 + Glue Catalog)
    |
Redshift Spectrum / Athena (consumption layer)
```

Orchestrated by AWS Step Functions (EMR -> dbt in sequence).

---

## Tech Stack

| Service | Role |
|---------|------|
| AWS EMR Serverless | Heavy processing (Bronze to Silver) with PySpark |
| dbt-core + dbt-athena | SQL transformations (Silver to Gold) |
| AWS Athena | SQL engine for dbt transformations |
| Apache Iceberg | Table format (time travel, schema evolution, ACID) |
| AWS S3 | Data lake storage |
| AWS Glue Catalog | Metastore for all tables |
| AWS ECS Fargate | Serverless container execution for dbt |
| AWS ECR | Docker image registry |
| AWS CodeBuild | CI/CD - builds Docker image in the cloud |
| AWS Step Functions | Pipeline orchestration |
| AWS Redshift Spectrum | Query engine for consumption |
| AWS CloudWatch | Execution logs |


---

## Project Structure

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
│   │   │   ├── stg_fedex_shipments.sql      # Reads from seed (testing)
│   │   │   ├── stg_fedex_silver.sql         # Reads from EMR output (production)
│   │   │   ├── schema.yml
│   │   │   └── sources.yml
│   │   ├── intermediate/
│   │   │   └── int_fedex_metricas.sql
│   │   └── marts/
│   │       └── mart_fedex_resumo_rota.sql   # Iceberg table on S3
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

## Data Flow (Medallion Architecture)

```
Bronze (EMR):  CSV -> filter invalid records -> Parquet partitioned by Year/Month
Silver (EMR):  + timestamp conversion, travel time, route ID, validations, delay categorization
Gold (dbt):    + aggregations by route, KPIs, final Iceberg tables for consumption
```

### DAG (dbt models)

```
source: fedex_silver (Glue Catalog, from EMR)
    |
staging: stg_fedex_silver (view) - rename columns, standardize
    |
intermediate: int_fedex_metricas (view) - business rules, classifications
    |
marts: mart_fedex_resumo_rota (Iceberg table on S3) - aggregated KPIs
    |
Redshift Spectrum / Athena / Dashboards
```

---

## Setup - Step by Step

### Prerequisites

- AWS account with access to: ECR, CodeBuild, ECS, EMR Serverless, Athena, S3, Glue, IAM, CloudWatch, Step Functions
- GitHub repository
- AWS CLI configured (SSO recommended)

### 1. Create S3 Bucket

```bash
aws s3api create-bucket --bucket [YOUR_BUCKET] --region us-east-1
```

### 2. Create Glue Database

Console: Glue -> Databases -> Add database -> Name: `dbt_landing`

### 3. Create ECR Repository

Console: ECR -> Create repository -> Name: `dbt-lakehouse`

### 4. Create CodeBuild Project

- Source: GitHub (PAT with repo scope)
- Environment: EC2, Container, Ubuntu, Standard 7.0, Privileged
- Env vars: `AWS_ACCOUNT_ID`, `IMAGE_REPO_NAME`
- After creating: attach `AmazonEC2ContainerRegistryPowerUser` to the role

### 5. Create EMR Serverless Application

```bash
aws emr-serverless create-application --release-label emr-7.1.0 --type SPARK --name "medallion-silver"
```

### 6. Create ECS Cluster

Console: ECS -> Clusters -> Create -> Fargate only -> Name: `dbt-cluster`

### 7. Create IAM Roles

- `ecsTaskDbtRole`: trust `ecs-tasks.amazonaws.com`, policies: AthenaFullAccess, S3FullAccess, GlueConsoleFullAccess

### 8. Create CloudWatch Log Group

- Name: `/ecs/dbt-validate`

### 9. Create Task Definition

- Fargate, 0.25 vCPU, 0.5 GB
- Task role: `ecsTaskDbtRole`
- Entry point: `sh,-c`
- Command: `dbt seed && dbt run && dbt test`

### 10. Create Step Functions State Machine

Upload `orchestration/step-functions-definition.json` with your values.

### 11. Run Pipeline

- CodeBuild -> Start build (creates Docker image)
- Step Functions -> Start execution (runs EMR then dbt)

---

## Consuming with Redshift Spectrum

```sql
CREATE EXTERNAL SCHEMA lakehouse
FROM DATA CATALOG
DATABASE 'dbt_landing'
IAM_ROLE 'arn:aws:iam::[ACCOUNT_ID]:role/[REDSHIFT_ROLE]'
REGION 'us-east-1';

SELECT * FROM lakehouse.mart_fedex_resumo_rota LIMIT 10;
```

---

## Common Issues

| Problem | Solution |
|---------|----------|
| Docker Hub 429 rate limit | Use `public.ecr.aws/docker/library/python:3.11-slim` |
| ECR repo not found | Ensure `IMAGE_REPO_NAME` env var matches ECR name |
| TYPE_MISMATCH in tests | Add `quote: false` in accepted_values |
| ENTRYPOINT conflict | Set Entry point: `sh,-c` in task definition |
| Permission denied | Attach Athena/S3/Glue policies to task role |
| NA values in numeric cols | Use `try_cast()` in staging SQL |

---

## License

MIT
