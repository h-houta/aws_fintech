# ── Athena Workgroup ─────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "fintech_enterprise" {
  name        = "fintech-enterprise-workgroup"
  description = "Athena workgroup for Fintech Enterprise fintech serverless queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}

# ── Athena Named Queries ──────────────────────────────────────────────────────────
# Pre-register sample queries in the console for easy access

resource "aws_athena_named_query" "top_merchants" {
  name        = "fintech-enterprise-top-merchants-by-volume"
  workgroup   = aws_athena_workgroup.fintech_enterprise.name
  database    = aws_glue_catalog_database.fintech_enterprise.name
  description = "Top 10 merchants by total transaction volume (AED)"

  query = file("${path.root}/../athena/queries/top_merchants.sql")
}

resource "aws_athena_named_query" "monthly_volume" {
  name        = "fintech-enterprise-monthly-transaction-volume"
  workgroup   = aws_athena_workgroup.fintech_enterprise.name
  database    = aws_glue_catalog_database.fintech_enterprise.name
  description = "Monthly transaction count and volume trend"

  query = file("${path.root}/../athena/queries/monthly_volume.sql")
}

resource "aws_athena_named_query" "currency_breakdown" {
  name        = "fintech-enterprise-fx-currency-breakdown"
  workgroup   = aws_athena_workgroup.fintech_enterprise.name
  database    = aws_glue_catalog_database.fintech_enterprise.name
  description = "FX exposure: transactions in non-AED currencies"

  query = file("${path.root}/../athena/queries/currency_breakdown.sql")
}
