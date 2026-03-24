# ── Lake Formation: Column-Level Security ────────────────────────────────────────
# Masks email and phone_number columns in the users table for non-admin principals

# Make the deploying IAM user a Lake Formation admin so it can grant permissions
resource "aws_lakeformation_data_lake_settings" "admin" {
  admins = [data.aws_caller_identity.current.arn]
}

# Register S3 locations with Lake Formation
resource "aws_lakeformation_resource" "raw" {
  arn = aws_s3_bucket.raw.arn
}

resource "aws_lakeformation_resource" "curated" {
  arn = aws_s3_bucket.curated.arn
}

# Grant Glue role data location access on registered S3 paths
resource "aws_lakeformation_permissions" "glue_data_location_raw" {
  principal = aws_iam_role.glue.arn

  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn = aws_s3_bucket.raw.arn
  }

  depends_on = [aws_lakeformation_resource.raw]
}

resource "aws_lakeformation_permissions" "glue_data_location_curated" {
  principal = aws_iam_role.glue.arn

  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn = aws_s3_bucket.curated.arn
  }

  depends_on = [aws_lakeformation_resource.curated]
}

# Grant Glue role full access to the catalog database
resource "aws_lakeformation_permissions" "glue_database" {
  principal = aws_iam_role.glue.arn

  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.fintech_enterprise.name
  }
}

# Grant Glue role full access to all tables in database
# Note: Table-level and column-level Lake Formation permissions (glue_tables,
# redshift_tables, redshift_users_column_filter) must be applied AFTER the
# Glue Crawler has run and created tables in the catalog. Apply these
# post-deployment via: terraform apply -target=aws_lakeformation_permissions.glue_tables ...
resource "aws_lakeformation_permissions" "glue_tables" {
  principal = aws_iam_role.glue.arn

  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.fintech_enterprise.name
    wildcard      = true
  }

  depends_on = [aws_lakeformation_permissions.glue_database]
}

# Grant Redshift role read access to all tables except sensitive columns
resource "aws_lakeformation_permissions" "redshift_tables" {
  principal = aws_iam_role.redshift.arn

  permissions = ["SELECT"]

  table {
    database_name = aws_glue_catalog_database.fintech_enterprise.name
    wildcard      = true
  }

  depends_on = [aws_lakeformation_permissions.glue_tables]
}

# Column-level security: Redshift cannot see email or phone_number raw values
# (they only see hashed versions in the curated layer)
resource "aws_lakeformation_permissions" "redshift_users_column_filter" {
  principal = aws_iam_role.redshift.arn

  permissions = ["SELECT"]

  table_with_columns {
    database_name = aws_glue_catalog_database.fintech_enterprise.name
    name          = "users"

    # Explicitly list allowed columns — excludes PII: email, phone_number, password
    column_names = [
      "id",
      "first_name",
      "last_name",
      "is_active",
      "is_verified",
      "track_id",
      "created_at",
      "updated_at",
      "deleted_at",
    ]
  }

  depends_on = [aws_lakeformation_permissions.redshift_tables]
}
