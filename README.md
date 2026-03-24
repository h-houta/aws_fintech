# Fintech Enterprise AWS Data Engineering Pipeline

> **Designed for enterprise-scale fintech transaction processing. Demonstrated with anonymized sample dataset.**

An end-to-end AWS data engineering pipeline for a UAE-based corporate card fintech. Ingests raw card transaction data, transforms it through a multi-stage ETL, loads it into both a serverless query layer (Athena) and a dimensional warehouse (Redshift Serverless), applies data quality checks, and serves a BI dashboard — all orchestrated by Apache Airflow running locally in Docker.

---

## Architecture

```
Raw CSVs (local)
     │
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  S3 Raw Landing (fintech-enterprise-data-lake-raw)                                │
│  orgs/ | users/ | cards/ | transactions/ | clearing/               │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  S3 PUT event
                           ▼
                    ┌─────────────┐
                    │   Lambda    │  s3_trigger → start Glue Crawler
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │Glue Crawler │  Schema discovery → Data Catalog
                    └──────┬──────┘
                           │
                           ▼
          ┌────────────────────────────────────┐
          │        Glue ETL Jobs (PySpark)     │
          │  Currency normalization            │
          │  Timestamp UTC normalization       │
          │  Merchant standardization          │
          │  PII masking (SHA-256)             │
          │  Type casting + deduplication      │
          └──────────────┬─────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  S3 Curated (fintech-enterprise-data-lake-curated) — Partitioned Parquet          │
│  transactions/year=*/month=*/ | clearing/period=*/                 │
│  cards/card_type=*/          | users/ | orgs/                      │
└──────────┬─────────────────────────────┬───────────────────────────┘
           │                             │
           ▼                             ▼
  ┌─────────────────┐          ┌──────────────────────┐
  │ Athena (SQL)    │          │ Redshift Serverless   │
  │ fintech-enterprise-workgroup  │          │ Star Schema (fintech_enterprise_dw) │
  │ Serverless      │          │ + Spectrum external   │
  └────────┬────────┘          └──────────┬────────────┘
           │                             │
           └──────────┬──────────────────┘
                      │
                      ▼
             ┌──────────────────┐
             │ Streamlit        │
             │ BI Dashboard     │
             └──────────────────┘

Orchestration: Apache Airflow (Docker) — fintech_enterprise_aws_pipeline DAG
Infrastructure: Terraform (all resources tagged Project: fintech-enterprise-aws-pipeline)
Security: IAM roles + Lake Formation column-level masking
Monitoring: CloudWatch alarms + SNS email alerts
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Storage | AWS S3 (raw + curated), S3 Glacier (archive) |
| Schema Discovery | AWS Glue Crawler + Data Catalog |
| ETL | AWS Glue PySpark 4.0 (5 jobs) |
| Event Trigger | AWS Lambda (Python 3.11) |
| Serverless Query | Amazon Athena (engine v3) |
| Data Warehouse | Amazon Redshift Serverless |
| Lakehouse | Redshift Spectrum (external schema → S3) |
| Data Quality | AWS Glue Data Quality |
| Orchestration | Apache Airflow 2.9 (local Docker) |
| BI Dashboard | Streamlit + Plotly |
| Infrastructure | Terraform 1.7+ |
| Security | IAM, AWS Lake Formation |
| Alerting | Amazon SNS + CloudWatch |

---

## Repository Structure

```
fintech-enterprise-aws-pipeline/
├── terraform/          # All AWS infrastructure as code
│   ├── main.tf         # Provider + backend config
│   ├── s3.tf           # Buckets + lifecycle policies
│   ├── glue.tf         # Crawler + ETL jobs
│   ├── redshift.tf     # Serverless namespace + workgroup
│   ├── athena.tf       # Workgroup + named queries
│   ├── lambda.tf       # S3 trigger + SNS topic
│   ├── iam.tf          # Glue / Redshift / Lambda roles
│   └── lake_formation.tf  # Column-level security
├── glue/etl/           # PySpark transform scripts (5 jobs)
├── lambda/s3_trigger/  # Event-driven pipeline trigger
├── redshift/
│   ├── ddl/            # Dimension + fact table DDL, Spectrum schema
│   ├── etl/            # COPY commands
│   └── queries/        # Analytical SQL (spend, FX, settlement, utilization)
├── athena/queries/     # Serverless SQL (merchants, volume, currency)
├── data_quality/       # Glue DQ rule definitions (JSON)
├── airflow/
│   ├── docker-compose.yml  # Local Airflow stack
│   └── dags/fintech_enterprise_aws_pipeline.py  # Orchestration DAG
├── streamlit/app.py    # 6-page BI dashboard
├── tests/              # pytest unit tests (transforms + DQ rules)
├── scripts/            # upload_to_s3.sh, deploy.sh, teardown.sh
└── docs/data_dictionary.md
```

---

## Prerequisites

- Python 3.11+
- Terraform >= 1.7
- AWS CLI v2 (configured with appropriate IAM permissions)
- Docker + Docker Compose
- `pip install -e ".[dev]"` (installs all dependencies)

---

## Quick Start

### 1. Configure AWS credentials
```bash
aws configure   # or set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars
cp .env.example .env
# Edit .env with your values
```

### 2. Prepare data
```bash
# Place your raw CSVs in data/raw/
# Anonymize PII before any uploads:
make anonymize
```

### 3. Deploy to AWS
```bash
make install-dev   # Install Python dependencies
make init          # Terraform init
make plan          # Review what will be created
make deploy        # Apply infrastructure + upload data
```

### 4. Start Airflow
```bash
make airflow-up
# Open: http://localhost:8080 (admin / admin)
# Trigger DAG: fintech_enterprise_aws_pipeline
```

### 5. Launch BI Dashboard (local)
```bash
make dashboard
# Opens: http://localhost:8501
```

### 6. Run tests
```bash
make test
```

---

## Pipeline Stages

| Stage | Component | Description |
|-------|-----------|-------------|
| 1 | S3 Raw | CSV landing zone with Glacier lifecycle (90 days) |
| 2 | Glue Crawler | Auto schema discovery → Data Catalog |
| 3 | Glue ETL | PySpark transforms: currency, timestamps, PII, merchants |
| 4 | S3 Curated | Partitioned Parquet output |
| 5a | Athena | Serverless SQL queries over Parquet |
| 5b | Redshift | Star schema warehouse + Spectrum lakehouse |
| 6 | Data Quality | 10 rule-based checks (nulls, FK integrity, ranges) |
| 7 | Airflow | DAG orchestration with retry + SNS alerting |
| 8 | Streamlit | 6-page BI dashboard |
| 9 | Lake Formation | Column-level security on PII fields |
| 10 | Terraform | All infra as code; tagged for cost tracking |

---

## Data Model

### Dimensional Schema (Redshift fintech_enterprise_dw)

```
dim_dates ──────────────┐
dim_organizations ──┐   │
dim_users           │   │
dim_cards ──────────┼───┼──── fact_transactions
dim_merchants ──────┘   │
                        └──── fact_clearing
```

**Distribution keys**: fact tables distkeyed on `card_key` to colocate with `dim_cards`.
**Sort keys**: `(date_key, status)` on fact tables for time-range + status filter performance.

---

## Data Quality Checks

| Check | Rule |
|-------|------|
| No null card_ids | `card_id IS NOT NULL` in transactions |
| Positive settled amounts | `billing_amount > 0` WHERE `status = 'SETTLED'` |
| Valid currencies | `transaction_currency IN ('AED','USD','EGP','GBP','EUR')` |
| No duplicate transaction IDs | `IsUnique "id"` |
| Referential integrity: transactions → cards | FK check |
| Referential integrity: cards → users | FK check |
| Settlement date ordering | `settlement_date >= transaction_timestamp` |
| No duplicate clearing IDs | `IsUnique "transaction_id"` in clearing |
| Valid dr_cr | `dr_cr IN ('DEBIT','CREDIT')` |
| Valid card status | `status IN ('ACTIVE','TERMINATED')` |

---

## Analytical Queries

### Athena (Serverless)
- `top_merchants.sql` — Top 10 merchants by AED volume
- `monthly_volume.sql` — Monthly transaction trend + decline rate
- `currency_breakdown.sql` — FX exposure by currency

### Redshift (Warehouse)
- `spend_by_org.sql` — Total spend + decline rate per organization
- `fx_exposure.sql` — Foreign currency risk with % of total portfolio
- `settlement_lag.sql` — P50/P95 settlement lag by country and currency
- `card_utilization.sql` — Active vs terminated card rates, dormancy

---

## Cost Estimate (AWS us-east-1)

| Service | Estimate |
|---------|---------|
| S3 (raw + curated, ~50 MB) | ~$0.01/mo |
| Glue Crawler (5 min, monthly) | ~$0.07/run |
| Glue ETL (5 jobs × 2 DPU × 10 min) | ~$0.15/run |
| Athena (per query, ~1 MB scanned) | ~$0.005/query |
| Redshift Serverless (8 RPU, dev only) | ~$0.36/hr active |
| Lambda (1 invocation, <1s) | ~$0.00002 |
| **Total (dev, infrequent use)** | **< $5/mo** |

> Run `make teardown` when done to stop all costs.

---

## Security

- **IAM**: Three least-privilege roles (`fintech-enterprise-glue-role`, `fintech-enterprise-redshift-role`, `fintech-enterprise-lambda-role`)
- **Lake Formation**: Column-level grants — Redshift role cannot access raw `email`, `phone_number`
- **PII Masking**: SHA-256 hashing applied in ETL before data reaches curated layer
- **Encryption**: SSE-S3 on all S3 buckets; no public access
- **Secrets**: Redshift admin password via Terraform variable (never hardcoded)

---

## Teardown

```bash
make teardown
# Empties all S3 buckets, destroys all Terraform resources
# Confirm by typing 'destroy' when prompted
```

---

## Author

Senior Data Engineer — UAE/Remote
Stack: Teradata · Oracle · SQL Server · Airflow · dbt · Kafka · AWS · Docker

*Portfolio project demonstrating production-grade AWS data engineering patterns.*
