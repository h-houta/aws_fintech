-- =============================================================================
-- Redshift Query: FX Exposure Analysis
-- Business purpose: Quantify currency risk for Fintech Enterprise's treasury team —
--                   which currencies and amounts are NOT settled in AED
-- =============================================================================

WITH fx_summary AS (
    SELECT
        transaction_currency,
        COUNT(*)                        AS transaction_count,
        SUM(transaction_amount)         AS total_original_amount,
        SUM(billing_amount)             AS total_billing_aed,
        SUM(usd_amount)                 AS total_usd_equivalent,
        AVG(transaction_amount)         AS avg_original_amount,
        MIN(transaction_timestamp)      AS first_seen,
        MAX(transaction_timestamp)      AS last_seen
    FROM fact_transactions
    WHERE status = 'SETTLED'
      AND transaction_currency IS NOT NULL
    GROUP BY transaction_currency
),
totals AS (
    SELECT SUM(total_billing_aed) AS grand_total_aed
    FROM fx_summary
)
SELECT
    f.transaction_currency,
    f.transaction_count,
    ROUND(f.total_original_amount, 2)          AS total_original,
    ROUND(f.total_billing_aed, 2)              AS total_billing_aed,
    ROUND(f.total_usd_equivalent, 2)           AS total_usd_equivalent,
    ROUND(f.avg_original_amount, 2)            AS avg_transaction,
    ROUND(100.0 * f.total_billing_aed / NULLIF(t.grand_total_aed, 0), 2) AS pct_of_total_spend,
    f.first_seen::DATE                         AS first_transaction_date,
    f.last_seen::DATE                          AS last_transaction_date,
    CASE WHEN f.transaction_currency = 'AED' THEN 'Domestic' ELSE 'Foreign' END AS exposure_type
FROM fx_summary f
CROSS JOIN totals t
ORDER BY f.total_billing_aed DESC;
