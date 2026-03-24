# AWS Console Screenshot Guide — Fintech Enterprise Portfolio

Region: **us-east-1** | Account: 352206182791

Open each link below, take a full-page screenshot, and save to `upwork_media/`.

## S3

1. **s3_buckets_list.png** — S3 → Buckets list (shows all 4 fintech-enterprise-* buckets)
2. **s3_raw_objects.png** — Click `fintech-enterprise-data-lake-raw` → shows 5 folders (cards/, clearing/, orgs/, transactions/, users/)
3. **s3_curated_objects.png** — Click `fintech-enterprise-data-lake-curated` → shows Parquet folders

## Glue

4. **glue_catalog_database.png** — Glue → Data Catalog → Databases → `fintech_enterprise_catalog`
5. **glue_catalog_tables.png** — Click `fintech_enterprise_catalog` → Tables (shows 10 tables: 5 raw + 5 curated)
6. **glue_crawlers.png** — Glue → Crawlers (shows fintech-enterprise-raw-crawler + fintech-enterprise-curated-crawler, both READY)
7. **glue_etl_jobs.png** — Glue → ETL jobs (shows 5 transform jobs)
8. **glue_job_run_history.png** — Click any job → Runs tab (shows SUCCEEDED runs)

## Lake Formation

9. **lakeformation_databases.png** — Lake Formation → Databases → fintech_enterprise_catalog
10. **lakeformation_permissions.png** — Lake Formation → Data permissions (shows Glue/Redshift grants)

## Athena

11. **athena_workgroup.png** — Athena → Workgroups → fintech-enterprise-workgroup
12. **athena_query_result.png** — Athena → Editor → run one of the saved queries (e.g. fintech-enterprise-top-merchants), show results

## Redshift

13. **redshift_namespace.png** — Redshift → Serverless → Namespace `fintech-enterprise-ns`
14. **redshift_workgroup.png** — Redshift → Serverless → Workgroup `fintech-enterprise-wg` (shows 8 RPU)
15. **redshift_query_editor.png** — Redshift → Query Editor V2 → run `SELECT * FROM fintech_enterprise_dw.fact_transactions LIMIT 10;`

## Lambda

16. **lambda_function.png** — Lambda → Functions → `fintech-enterprise-s3-glue-trigger` (shows trigger config)
17. **lambda_s3_trigger.png** — Configuration tab → Triggers (shows S3 event source)

## CloudWatch

18. **cloudwatch_alarms.png** — CloudWatch → Alarms (shows 4 alarms: 3 billing + 1 Glue failure)

## SNS

19. **sns_topic.png** — SNS → Topics → `fintech-enterprise-pipeline-alerts`
20. **sns_subscriptions.png** — Click topic → Subscriptions (shows confirmed email)

## IAM

21. **iam_roles.png** — IAM → Roles → filter "fintech_enterprise" (shows 3 roles: glue, redshift, lambda)

## Terraform (terminal, not console)

22. **terraform_plan.png** — Terminal: `terraform plan` output (already captured as text)

---

**Tips:**
- Use browser zoom 90% for wider screenshots
- Crop to remove personal AWS account info from the top nav bar if desired
- Dark mode is fine but light mode photographs better for portfolios
