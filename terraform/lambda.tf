# ── Lambda: S3 → Glue Trigger ─────────────────────────────────────────────────────
# Triggered by S3 PUT on raw/ prefix → starts Glue Crawler → then Glue ETL jobs

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.root}/../lambda/s3_trigger/"
  output_path = "${path.root}/../lambda/s3_trigger.zip"
}

resource "aws_lambda_function" "s3_trigger" {
  function_name    = "fintech-enterprise-s3-glue-trigger"
  description      = "Triggered on S3 raw upload → starts Glue Crawler + ETL pipeline"
  runtime          = "python3.11"
  handler          = "handler.lambda_handler"
  role             = aws_iam_role.lambda.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      GLUE_CRAWLER_NAME         = aws_glue_crawler.raw.name
      GLUE_JOB_TRANSACTIONS     = aws_glue_job.transform_transactions.name
      GLUE_JOB_CLEARING         = aws_glue_job.transform_clearing.name
      GLUE_JOB_CARDS            = aws_glue_job.transform_cards.name
      GLUE_JOB_USERS            = aws_glue_job.transform_users.name
      GLUE_JOB_ORGS             = aws_glue_job.transform_orgs.name
      SNS_ALERT_TOPIC_ARN       = aws_sns_topic.pipeline_alerts.arn
      RAW_BUCKET                = aws_s3_bucket.raw.bucket
      CURATED_BUCKET            = aws_s3_bucket.curated.bucket
    }
  }
}

# Allow S3 to invoke Lambda
resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

# S3 bucket notification → Lambda on ObjectCreated
resource "aws_s3_bucket_notification" "raw_trigger" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = ""
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}

# ── SNS Topic for Alerts ──────────────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "fintech-enterprise-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.sns_alert_email
}
