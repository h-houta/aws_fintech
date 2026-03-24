# ── Redshift Serverless ───────────────────────────────────────────────────────────
# Using Serverless to avoid provisioned cluster costs (~$0.36/RPU-hour vs ~$180/mo min)

resource "aws_redshiftserverless_namespace" "fintech_enterprise" {
  namespace_name      = var.redshift_namespace
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password

  iam_roles = [aws_iam_role.redshift.arn]

  tags = {
    Name = "fintech-enterprise-redshift-namespace"
  }
}

resource "aws_redshiftserverless_workgroup" "fintech_enterprise" {
  namespace_name = aws_redshiftserverless_namespace.fintech_enterprise.namespace_name
  workgroup_name = var.redshift_workgroup

  # Base RPU capacity — minimum for dev (8 RPU = ~$0.36/hr when active only)
  base_capacity = 8

  publicly_accessible = false

  tags = {
    Name = "fintech-enterprise-redshift-workgroup"
  }

  depends_on = [aws_redshiftserverless_namespace.fintech_enterprise]
}
