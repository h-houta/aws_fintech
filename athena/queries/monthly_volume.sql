-- =============================================================================
-- Athena Query: Monthly Transaction Volume Trend
-- Workgroup: fintech-enterprise-workgroup
-- Database: fintech_enterprise_catalog
-- Business purpose: Track monthly spend velocity to support financial reporting
--                   and anomaly detection (e.g., sudden volume spikes)
-- =============================================================================

SELECT
    year,
    month,
    CONCAT(year, '-', month)                                AS year_month,
    COUNT(*)                                                AS transaction_count,
    COUNT(DISTINCT card_id)                                 AS active_cards,
    ROUND(SUM(CAST(billing_amount AS DOUBLE)), 2)           AS total_spend_aed,
    ROUND(SUM(CAST(usd_amount AS DOUBLE)), 2)               AS total_spend_usd,
    ROUND(AVG(CAST(billing_amount AS DOUBLE)), 2)           AS avg_transaction_aed,
    SUM(CASE WHEN status = 'SETTLED'  THEN 1 ELSE 0 END)   AS settled_count,
    SUM(CASE WHEN status = 'DECLINED' THEN 1 ELSE 0 END)   AS declined_count,
    SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)   AS refunded_count,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'DECLINED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    )                                                       AS decline_rate_pct
FROM "fintech_enterprise_catalog"."transactions"
WHERE year IS NOT NULL
GROUP BY year, month
ORDER BY year, month;
