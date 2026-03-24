-- =============================================================================
-- Athena Query: Top Merchants by Transaction Volume
-- Workgroup: fintech-enterprise-workgroup
-- Database: fintech_enterprise_catalog (Glue Data Catalog → curated S3 Parquet)
-- Business purpose: Identify highest-volume merchants for spend analytics
--                   and potential merchant-funded rewards programs
-- =============================================================================

SELECT
    merchant_normalized                                     AS merchant_name,
    merchant_country,
    mcc,
    COUNT(*)                                                AS transaction_count,
    COUNT(DISTINCT card_id)                                 AS unique_cards,
    ROUND(SUM(CAST(billing_amount AS DOUBLE)), 2)           AS total_spend_aed,
    ROUND(SUM(CAST(usd_amount AS DOUBLE)), 2)               AS total_spend_usd,
    ROUND(AVG(CAST(billing_amount AS DOUBLE)), 2)           AS avg_transaction_aed
FROM "fintech_enterprise_catalog"."transactions"
WHERE status = 'SETTLED'
  AND merchant_normalized IS NOT NULL
GROUP BY
    merchant_normalized,
    merchant_country,
    mcc
ORDER BY total_spend_aed DESC
LIMIT 10;
