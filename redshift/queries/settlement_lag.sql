-- =============================================================================
-- Redshift Query: Settlement Lag Analysis
-- Business purpose: Measure how long it takes for transactions to clear —
--                   critical for cash flow forecasting and reconciliation SLAs
-- Source: fact_clearing (Mastercard network settlement data)
-- =============================================================================

SET search_path TO fintech_enterprise_dw;

WITH settlement_lags AS (
    SELECT
        fc.clearing_id,
        fc.merchant_name,
        fc.merchant_country,
        fc.transaction_amount,
        fc.transaction_currency,
        fc.settlement_status,
        fc.transaction_timestamp_utc,
        fc.capture_date_utc,
        fc.settlement_date_utc,
        DATEDIFF(
            HOUR,
            fc.transaction_timestamp_utc,
            fc.capture_date_utc
        )                                           AS hours_to_capture,
        DATEDIFF(
            DAY,
            fc.capture_date_utc,
            fc.settlement_date_utc
        )                                           AS days_capture_to_settle
    FROM fact_clearing fc
    WHERE fc.settlement_date_utc IS NOT NULL
      AND fc.capture_date_utc IS NOT NULL
      AND fc.transaction_timestamp_utc IS NOT NULL
),
lag_stats AS (
    SELECT
        merchant_country,
        transaction_currency,
        COUNT(*)                                    AS transaction_count,
        AVG(hours_to_capture)                       AS avg_hours_to_capture,
        AVG(days_capture_to_settle)                 AS avg_days_to_settle,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_capture_to_settle)  AS median_days,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY days_capture_to_settle) AS p95_days,
        MAX(days_capture_to_settle)                 AS max_days_to_settle
    FROM settlement_lags
    GROUP BY merchant_country, transaction_currency
)
SELECT
    merchant_country,
    transaction_currency,
    transaction_count,
    ROUND(avg_hours_to_capture, 1)  AS avg_hours_to_capture,
    ROUND(avg_days_to_settle, 1)    AS avg_days_to_settle,
    ROUND(median_days, 1)           AS median_days,
    ROUND(p95_days, 1)              AS p95_days,
    max_days_to_settle
FROM lag_stats
ORDER BY transaction_count DESC;
