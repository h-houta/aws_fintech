-- =============================================================================
-- Athena Query: Monthly FX Exposure Trend
-- Workgroup: fintech-enterprise-workgroup
-- Database: fintech_enterprise_catalog (Glue Data Catalog → curated S3 Parquet)
-- Business purpose: Track how foreign currency exposure shifts month-over-month
--                   Leverages Athena partition pruning on year/month columns
--                   for cost-efficient scans over the data lake
-- Complements: redshift/queries/fx_exposure.sql (point-in-time snapshot)
-- =============================================================================

WITH monthly_currency AS (
    SELECT
        year,
        month,
        transaction_currency,
        CASE
            WHEN transaction_currency = 'AED' THEN 'Domestic'
            ELSE 'Foreign'
        END                                                     AS exposure_type,
        COUNT(*)                                                AS txn_count,
        ROUND(SUM(CAST(billing_amount AS DOUBLE)), 2)           AS total_billing_aed,
        ROUND(SUM(CAST(usd_amount AS DOUBLE)), 2)               AS total_usd_equivalent,
        ROUND(AVG(CAST(billing_amount AS DOUBLE)), 2)           AS avg_billing_aed
    FROM "fintech_enterprise_catalog"."transactions"
    WHERE status = 'SETTLED'
      AND transaction_currency IS NOT NULL
      AND year IS NOT NULL
    GROUP BY year, month, transaction_currency
),
monthly_totals AS (
    SELECT
        year,
        month,
        SUM(total_billing_aed)                                  AS month_total_aed
    FROM monthly_currency
    GROUP BY year, month
)
SELECT
    mc.year,
    mc.month,
    CONCAT(mc.year, '-', LPAD(mc.month, 2, '0'))               AS year_month,
    mc.transaction_currency,
    mc.exposure_type,
    mc.txn_count,
    mc.total_billing_aed,
    mc.total_usd_equivalent,
    mc.avg_billing_aed,
    ROUND(100.0 * mc.total_billing_aed / NULLIF(mt.month_total_aed, 0), 2)
                                                                AS pct_of_monthly_spend,
    -- Month-over-month change in currency volume (AED)
    LAG(mc.total_billing_aed) OVER (
        PARTITION BY mc.transaction_currency
        ORDER BY mc.year, mc.month
    )                                                           AS prev_month_aed,
    ROUND(
        100.0 * (mc.total_billing_aed - LAG(mc.total_billing_aed) OVER (
            PARTITION BY mc.transaction_currency
            ORDER BY mc.year, mc.month
        )) / NULLIF(LAG(mc.total_billing_aed) OVER (
            PARTITION BY mc.transaction_currency
            ORDER BY mc.year, mc.month
        ), 0),
        2
    )                                                           AS mom_change_pct
FROM monthly_currency mc
JOIN monthly_totals mt ON mc.year = mt.year AND mc.month = mt.month
ORDER BY mc.year, mc.month, mc.total_billing_aed DESC;