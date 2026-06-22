# Orchestration - Step Functions

## Flow

```
EventBridge (schedule/trigger)
    |
Step Functions
    ├── 1. EMR Serverless -> camada_silver.py -> Parquet on S3 + Glue Catalog
    ├── 2. ECS Fargate -> dbt run/test -> Iceberg Marts on S3
    └── Success / Failure
```

## Setup

### 1. Create IAM Role for Step Functions

Trust entity: `states.amazonaws.com`

Policies:
- `AmazonEMRServerlessFullAccess`
- `AmazonECS_FullAccess`
- `CloudWatchLogsFullAccess`

Plus inline policy for `iam:PassRole` on EMR and ECS roles.

### 2. Create State Machine

Console: Step Functions -> Create state machine
- Upload `step-functions-definition.json`
- Replace all `[PLACEHOLDERS]` with your values

### 3. Schedule (optional)

```bash
aws events put-rule \
  --name lakehouse-pipeline-daily \
  --schedule-expression "cron(0 8 * * ? *)"
```

## Placeholders to Replace

| Placeholder | Description |
|-------------|-------------|
| `[EMR_APPLICATION_ID]` | EMR Serverless application ID |
| `[ACCOUNT_ID]` | Your AWS Account ID (12 digits) |
| `[YOUR_BUCKET]` | S3 bucket name |
| `[SUBNET_ID]` | Public subnet ID |
| `[SG_ID]` | Security group ID |
