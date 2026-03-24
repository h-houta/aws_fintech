variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project tag applied to every resource"
  type        = string
  default     = "fintech-enterprise-aws-pipeline"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "S3 bucket for raw CSV landing zone"
  type        = string
  default     = "fintech-enterprise-data-lake-raw"
}

variable "curated_bucket_name" {
  description = "S3 bucket for curated Parquet output"
  type        = string
  default     = "fintech-enterprise-data-lake-curated"
}

variable "athena_results_bucket" {
  description = "S3 bucket for Athena query results"
  type        = string
  default     = "fintech-enterprise-athena-results"
}

variable "glue_scripts_bucket" {
  description = "S3 bucket for Glue ETL PySpark scripts"
  type        = string
  default     = "fintech-enterprise-glue-scripts"
}

variable "glue_database_name" {
  description = "Glue Data Catalog database name"
  type        = string
  default     = "fintech_enterprise_catalog"
}

variable "redshift_namespace" {
  description = "Redshift Serverless namespace name"
  type        = string
  default     = "fintech-enterprise-ns"
}

variable "redshift_workgroup" {
  description = "Redshift Serverless workgroup name"
  type        = string
  default     = "fintech-enterprise-wg"
}

variable "redshift_db_name" {
  description = "Redshift database name"
  type        = string
  default     = "fintech_enterprise_dw"
}

variable "redshift_admin_user" {
  description = "Redshift admin username"
  type        = string
  default     = "fintech_enterprise_admin"
  sensitive   = true
}

variable "redshift_admin_password" {
  description = "Redshift admin password (min 8 chars, uppercase + number)"
  type        = string
  sensitive   = true
}

variable "glacier_transition_days" {
  description = "Days before raw S3 objects transition to Glacier"
  type        = number
  default     = 90
}

variable "sns_alert_email" {
  description = "Email address for pipeline failure alerts"
  type        = string
  default     = "alerts@example.com"
}

variable "account_id" {
  description = "AWS account ID (populated via data source)"
  type        = string
  default     = ""
}
