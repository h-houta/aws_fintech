# Upwork Portfolio — Media Capture Checklist

Capture at 2x/Retina resolution (min 2000px wide). Upwork displays at 1000x750 (4:3).
Use light mode for AWS Console. Redact account IDs, passwords, PII.
File naming: `NN_description.png` (e.g., `04_s3_bucket_list.png`).

---

## TIER 1 — Must-Have (capture first)

### Architecture & Overview
- [ ] 01. Architecture diagram (already at `00_architecture_diagram.png`, revisit for polish)
- [ ] 02. AWS Resource Groups / Tag Editor filtered by `Project = fintech-enterprise-aws-pipeline`
- [ ] 03. Data lineage diagram: raw CSVs -> ETL stages -> Redshift star schema

### S3 (Data Lake)
- [ ] 04. S3 Console — all 4 fintech_enterprise buckets listed
- [ ] 05. S3 Raw bucket — folder structure (orgs/, users/, cards/, transactions/, clearing/)
- [ ] 06. S3 Raw bucket — one CSV file detail (SSE encryption, metadata tags)
- [ ] 07. S3 Curated bucket — Parquet output with partitioned folders
- [ ] 08. S3 Bucket Properties — versioning enabled
- [ ] 09. S3 Bucket Properties — SSE-S3 encryption
- [ ] 10. S3 Lifecycle rules — Glacier transition (90 days) + Athena results expiration
- [ ] 11. S3 Permissions — Block Public Access (all 4 settings enabled)
- [ ] 12. S3 Event notification — Lambda trigger on `s3:ObjectCreated:*` (.csv)

### Glue (ETL & Catalog)
- [ ] 13. Glue Data Catalog — `fintech_enterprise_catalog` database
- [ ] 14. Glue Catalog — all 5 tables listed with column counts
- [ ] 15. Glue Catalog — `transactions` table schema detail
- [ ] 16. Glue Crawler — `fintech-enterprise-raw-crawler` config (S3 targets, recrawl policy)
- [ ] 17. Glue Crawler — run history showing SUCCEEDED + 5 tables
- [ ] 18. Glue ETL Jobs — all 5 transform jobs listed
- [ ] 19. Glue ETL Job — `fintech-enterprise-transform-transactions` run detail (SUCCEEDED, DPU hours)
- [ ] 20. Glue ETL Job — script tab showing PySpark code
- [ ] 21. Glue ETL Job — Spark UI execution plan
- [ ] 22. Glue ETL Job — CloudWatch monitoring metrics (bytes read/written, duration)
- [ ] 23. Glue Data Quality — rule definitions
- [ ] 24. Glue Data Quality — pass/fail results

### Athena
- [ ] 25. Athena — `fintech-enterprise-workgroup` config (result location, encryption, engine v3)
- [ ] 26. Athena — 3 named queries listed
- [ ] 27. Athena — `top_merchants.sql` running with results + data scanned
- [ ] 28. Athena — `monthly_volume.sql` running with results
- [ ] 29. Athena — `currency_breakdown.sql` running with results
- [ ] 30. Athena — query stats tab (data scanned: raw CSV vs Parquet comparison)

### Redshift Serverless
- [ ] 31. Redshift — `fintech-enterprise-ns` namespace overview (DB name, IAM roles)
- [ ] 32. Redshift — `fintech-enterprise-wg` workgroup (8 RPU, publicly_accessible=false)
- [ ] 33. Redshift Query Editor — external schema (Spectrum from Glue Catalog)
- [ ] 34. Redshift Query Editor — dim + fact tables after DDL
- [ ] 35. Redshift — `spend_by_org.sql` with results
- [ ] 36. Redshift — `fx_exposure.sql` with results
- [ ] 37. Redshift — `settlement_lag.sql` with results
- [ ] 38. Redshift — `card_utilization.sql` with results
- [ ] 39. Redshift — COPY command execution with row counts
- [ ] 40. Redshift — EXPLAIN output for one analytical query

### Lambda
- [ ] 41. Lambda — `fintech-enterprise-s3-glue-trigger` overview (runtime, memory, timeout)
- [ ] 42. Lambda — trigger tab showing S3 event source
- [ ] 43. Lambda — code view in console
- [ ] 44. Lambda — CloudWatch logs showing successful S3 trigger
- [ ] 45. Lambda — monitoring metrics (invocations, duration, errors)

### CloudWatch & Monitoring
- [ ] 46. CloudWatch — all alarms listed (billing $5/$10/$50 + Glue failure)
- [ ] 47. CloudWatch — one billing alarm detail
- [ ] 48. CloudWatch — Glue failure alarm detail
- [ ] 49. CloudWatch Logs — Lambda log group with successful invocation
- [ ] 50. CloudWatch Logs — Glue ETL job Spark execution logs

### IAM & Security
- [ ] 51. IAM — 3 fintech_enterprise roles listed
- [ ] 52. IAM — Glue role policy summary (least-privilege)
- [ ] 53. IAM — Lambda role policy
- [ ] 54. IAM — Redshift role policy

### Lake Formation (Governance)
- [ ] 55. Lake Formation — data lake admin settings
- [ ] 56. Lake Formation — registered S3 locations (raw + curated)
- [ ] 57. Lake Formation — database permissions (Glue=ALL, Redshift=SELECT)
- [ ] 58. Lake Formation — column-level security on `users` table (PII excluded)
- [ ] 59. Lake Formation — full data permissions summary view

### SNS (Alerting)
- [ ] 60. SNS — `fintech-enterprise-pipeline-alerts` topic overview
- [ ] 61. SNS — confirmed email subscription
- [ ] 62. SNS — sample alert email received (if available)

---

## TIER 2 — High Value

### Airflow (Docker Orchestration)
- [ ] 63. Airflow — DAG list showing `fintech_enterprise_aws_pipeline` with schedule
- [ ] 64. Airflow — Graph view showing full task flow with dependencies
- [ ] 65. Airflow — Grid/Tree view with multiple green successful runs
- [ ] 66. Airflow — expanded task group (5 parallel Glue ETL tasks)
- [ ] 67. Airflow — successful task log output
- [ ] 68. Airflow — Gantt chart showing parallelism

### Streamlit Dashboard
- [ ] 69. Streamlit — header with KPI metrics (Total Spend, Transactions, Settled, Declined)
- [ ] 70. Streamlit — sidebar (branding, data source selector)
- [ ] 71. Streamlit — Tab 1: Spend by Org (bar chart)
- [ ] 72. Streamlit — Tab 2: Volume Over Time (line + bar charts)
- [ ] 73. Streamlit — Tab 3: FX Exposure (donut chart + table)
- [ ] 74. Streamlit — Tab 4: Top Merchants (bar chart)
- [ ] 75. Streamlit — Tab 5: Card Utilization (gauge + pie chart)
- [ ] 76. Streamlit — Tab 6: Settlement Lag (histogram + metrics)

### Terraform (IaC)
- [ ] 77. Terminal — `terraform plan` output (57 resources)
- [ ] 78. Terminal — `terraform apply` output (Apply complete!)
- [ ] 79. Terminal — `terraform output` (redact sensitive values)
- [ ] 80. IDE — Terraform file tree (11 .tf files)
- [ ] 81. IDE — `s3.tf` showing lifecycle/versioning/encryption
- [ ] 82. IDE — `lake_formation.tf` showing column-level security HCL
- [ ] 83. Terminal — `terraform state list`

### Code Quality
- [ ] 84. IDE — full project file tree
- [ ] 85. IDE — one Glue ETL PySpark script
- [ ] 86. IDE — Lambda `handler.py`
- [ ] 87. IDE — Redshift DDL (star schema)
- [ ] 88. IDE — `glue_dq_rules.json` (10 rules)
- [ ] 89. IDE — `pyproject.toml`
- [ ] 90. IDE — `Makefile` targets

### Terminal Output
- [ ] 91. Terminal — `aws sts get-caller-identity` (live AWS connection)
- [ ] 92. Terminal — `make test` pytest output (passing)
- [ ] 93. Terminal — S3 upload script (5/5 uploaded)

---

## TIER 3 — Completeness & Polish

### AWS Billing & Cost
- [ ] 94. Cost Explorer — service breakdown (S3, Glue, Redshift, Lambda, Athena)
- [ ] 95. Cost Explorer — daily cost trend
- [ ] 96. Billing — total project cost (target: under $20)
- [ ] 97. Free Tier usage dashboard

### Video & Interactive
- [ ] 98. Loom video (3-5 min) — full pipeline walkthrough: architecture -> Terraform -> S3 upload -> Glue -> Athena -> Redshift -> Streamlit
- [ ] 99. Loom video (60s) — elevator pitch: architecture + dashboard result
- [ ] 100. GIF (15s) — pipeline trigger in action: upload CSV -> Lambda fires -> Glue starts

### Designed Graphics
- [ ] 101. Before/after comparison: Raw CSV -> Curated Parquet (file sizes, schema)
- [ ] 102. Performance metrics summary: query times, data scanned, ETL duration
- [ ] 103. Data quality scorecard: 10/10 rules passed
- [ ] 104. Cost summary card: total cost, cost per run, savings measures

### GitHub
- [ ] 105. GitHub — rendered README.md with architecture diagram
- [ ] 106. GitHub — repo root file listing
- [ ] 107. GitHub — commit history (clean messages)

---

## Capture Tips
1. Resolution: 2x/Retina, min 2000px wide (Upwork displays at 1000x750, 4:3 ratio)
2. Browser: light mode for all AWS Console screenshots
3. Redact: account IDs, passwords, emails, real PII in every screenshot
4. Annotate: add callout boxes/arrows on key screenshots (e.g., "Column-level PII exclusion")
5. NDA compliance: verify no real company names or PII visible in any screenshot