# ── Glue Data Catalog Database ───────────────────────────────────────────────────

resource "aws_glue_catalog_database" "fintech_enterprise" {
  name        = var.glue_database_name
  description = "Fintech Enterprise fintech data catalog — shared by Athena and Redshift Spectrum"
}

# ── Glue Crawler ─────────────────────────────────────────────────────────────────
# Discovers schemas from raw CSVs and registers them in the Data Catalog

resource "aws_glue_crawler" "raw" {
  name          = "fintech-enterprise-raw-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.fintech_enterprise.name
  description   = "Crawls raw S3 CSVs and registers schemas in Glue Catalog"

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/orgs/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/users/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/cards/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/transactions/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/clearing/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
      Tables     = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "LOG"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }
}

# ── Glue ETL Jobs ─────────────────────────────────────────────────────────────────

locals {
  glue_job_defaults = {
    role_arn          = aws_iam_role.glue.arn
    glue_version      = "4.0"
    worker_type       = "G.1X"
    number_of_workers = 2
    timeout           = 60  # minutes

    default_arguments = {
      "--job-language"                     = "python"
      "--enable-metrics"                   = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-spark-ui"                  = "true"
      "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_scripts.bucket}/spark-logs/"
      "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.bucket}/temp/"
      "--RAW_BUCKET"                       = aws_s3_bucket.raw.bucket
      "--CURATED_BUCKET"                   = aws_s3_bucket.curated.bucket
    }
  }
}

resource "aws_glue_job" "transform_transactions" {
  name         = "fintech-enterprise-transform-transactions"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  description  = "Normalise currencies, clean merchants, cast types for transactions table"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/etl/transform_transactions.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  default_arguments = merge(local.glue_job_defaults.default_arguments, {
    "--job-bookmark-option" = "job-bookmark-enable"
  })

  depends_on = [aws_s3_object.glue_transform_transactions]
}

resource "aws_glue_job" "transform_clearing" {
  name         = "fintech-enterprise-transform-clearing"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  description  = "Normalise timestamps, clean settlement fields for clearing table"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/etl/transform_clearing.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  default_arguments = local.glue_job_defaults.default_arguments

  depends_on = [aws_s3_object.glue_transform_clearing]
}

resource "aws_glue_job" "transform_cards" {
  name         = "fintech-enterprise-transform-cards"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  description  = "Cast types and partition by card_type for cards table"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/etl/transform_cards.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  default_arguments = local.glue_job_defaults.default_arguments

  depends_on = [aws_s3_object.glue_transform_cards]
}

resource "aws_glue_job" "transform_users" {
  name         = "fintech-enterprise-transform-users"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  description  = "Mask PII (email, phone) and write curated users"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/etl/transform_users.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  default_arguments = local.glue_job_defaults.default_arguments

  depends_on = [aws_s3_object.glue_transform_users]
}

resource "aws_glue_job" "transform_orgs" {
  name         = "fintech-enterprise-transform-orgs"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  description  = "Cast types and write curated organizations"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/etl/transform_orgs.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  default_arguments = local.glue_job_defaults.default_arguments

  depends_on = [aws_s3_object.glue_transform_orgs]
}

# ── CloudWatch Alarms for Glue Job Failures ───────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "glue_transactions_failure" {
  alarm_name          = "fintech-enterprise-glue-transactions-failure"
  alarm_description   = "Alert when transactions Glue job fails"
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = aws_glue_job.transform_transactions.name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}
