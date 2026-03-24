# ── S3 Buckets ──────────────────────────────────────────────────────────────────
# Raw landing zone, curated Parquet store, Athena results, Glue scripts

resource "aws_s3_bucket" "raw" {
  bucket = "${var.raw_bucket_name}-${local.account_id}"

  lifecycle {
    prevent_destroy = false  # Set true in production
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: transition raw CSVs to Glacier after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "glacier-transition"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = var.glacier_transition_days
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# ── Curated (Parquet) Bucket ─────────────────────────────────────────────────────

resource "aws_s3_bucket" "curated" {
  bucket = "${var.curated_bucket_name}-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Athena Results Bucket ────────────────────────────────────────────────────────

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.athena_results_bucket}-${local.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Expire Athena result files after 30 days (cost saving)
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 30
    }
  }
}

# ── Glue Scripts Bucket ──────────────────────────────────────────────────────────

resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.glue_scripts_bucket}-${local.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload Glue ETL scripts to S3 (runs after Terraform apply)
resource "aws_s3_object" "glue_transform_transactions" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "etl/transform_transactions.py"
  source = "${path.root}/../glue/etl/transform_transactions.py"
  etag   = filemd5("${path.root}/../glue/etl/transform_transactions.py")
}

resource "aws_s3_object" "glue_transform_clearing" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "etl/transform_clearing.py"
  source = "${path.root}/../glue/etl/transform_clearing.py"
  etag   = filemd5("${path.root}/../glue/etl/transform_clearing.py")
}

resource "aws_s3_object" "glue_transform_cards" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "etl/transform_cards.py"
  source = "${path.root}/../glue/etl/transform_cards.py"
  etag   = filemd5("${path.root}/../glue/etl/transform_cards.py")
}

resource "aws_s3_object" "glue_transform_users" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "etl/transform_users.py"
  source = "${path.root}/../glue/etl/transform_users.py"
  etag   = filemd5("${path.root}/../glue/etl/transform_users.py")
}

resource "aws_s3_object" "glue_transform_orgs" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "etl/transform_orgs.py"
  source = "${path.root}/../glue/etl/transform_orgs.py"
  etag   = filemd5("${path.root}/../glue/etl/transform_orgs.py")
}
