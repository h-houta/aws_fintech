-- =============================================================================
-- Redshift Spectrum: External Schema (Lakehouse Pattern)
-- Project: Fintech Enterprise AWS Data Pipeline
-- Purpose: Query curated S3 Parquet directly from Redshift without COPY
--          Enables lakehouse queries joining Redshift tables with S3 data
-- Prerequisites:
--   - Replace {ACCOUNT_ID} with your AWS account ID
--   - Replace {REGION} with your AWS region (e.g., us-east-1)
--   - IAM role fintech-enterprise-redshift-role must be attached to the Redshift namespace
-- =============================================================================

-- Drop existing external schema if re-running
DROP SCHEMA IF EXISTS fintech_enterprise_spectrum CASCADE;

-- Create external schema pointing to Glue Data Catalog
CREATE EXTERNAL SCHEMA fintech_enterprise_spectrum
FROM DATA CATALOG
DATABASE 'fintech_enterprise_catalog'
IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/fintech-enterprise-redshift-role'
REGION '{REGION}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Verify external tables are visible:
-- SELECT schemaname, tablename FROM SVV_EXTERNAL_TABLES WHERE schemaname = 'fintech_enterprise_spectrum';

-- =============================================================================
-- Example: Lakehouse join — Redshift dim + Spectrum fact
-- =============================================================================
-- SELECT
--     o.business_name,
--     SUM(t.billing_amount) AS total_spend_aed
-- FROM fintech_enterprise_spectrum.transactions t
-- JOIN fintech_enterprise_dw.dim_organizations o ON t.financial_account_id = o.org_id
-- WHERE t.status = 'SETTLED'
-- GROUP BY o.business_name
-- ORDER BY total_spend_aed DESC;
