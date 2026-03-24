output "raw_bucket_name" {
  description = "S3 raw landing bucket"
  value       = aws_s3_bucket.raw.bucket
}

output "curated_bucket_name" {
  description = "S3 curated Parquet bucket"
  value       = aws_s3_bucket.curated.bucket
}

output "athena_workgroup_name" {
  description = "Athena workgroup"
  value       = aws_athena_workgroup.fintech_enterprise.name
}

output "glue_database_name" {
  description = "Glue Data Catalog database"
  value       = aws_glue_catalog_database.fintech_enterprise.name
}

output "glue_crawler_name" {
  description = "Glue crawler for raw CSV discovery"
  value       = aws_glue_crawler.raw.name
}

output "redshift_workgroup_endpoint" {
  description = "Redshift Serverless workgroup endpoint"
  value       = aws_redshiftserverless_workgroup.fintech_enterprise.endpoint
  sensitive   = true
}

output "redshift_namespace_id" {
  description = "Redshift Serverless namespace ID"
  value       = aws_redshiftserverless_namespace.fintech_enterprise.id
}

output "lambda_trigger_arn" {
  description = "Lambda function ARN for S3→Glue trigger"
  value       = aws_lambda_function.s3_trigger.arn
}

output "sns_alert_topic_arn" {
  description = "SNS topic ARN for pipeline failure alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  value       = aws_iam_role.glue.arn
}
