-- =============================================================================
-- Redshift Query: Total Spend by Organization
-- Business purpose: Identify which organizations are driving the most card
--                   spend to support account management and credit risk teams
-- =============================================================================

WITH org_spend AS (
    SELECT
        o.org_id,
        o.business_name,
        o.country,
        COUNT(DISTINCT t.transaction_id)                        AS total_transactions,
        COUNT(DISTINCT t.card_key)                              AS unique_cards_used,
        SUM(t.billing_amount)                                   AS total_spend_aed,
        SUM(t.usd_amount)                                       AS total_spend_usd,
        AVG(t.billing_amount)                                   AS avg_transaction_aed,
        SUM(CASE WHEN t.status = 'SETTLED' THEN t.billing_amount ELSE 0 END) AS settled_amount_aed,
        SUM(CASE WHEN t.status = 'REFUNDED' THEN t.billing_amount ELSE 0 END) AS refunded_amount_aed,
        SUM(CASE WHEN t.status = 'DECLINED' THEN 1 ELSE 0 END) AS declined_count
    FROM fact_transactions t
    JOIN dim_organizations o ON t.org_key = o.org_key
    WHERE t.status IN ('SETTLED', 'REFUNDED')
    GROUP BY o.org_id, o.business_name, o.country
)
SELECT
    business_name,
    country,
    total_transactions,
    unique_cards_used,
    ROUND(total_spend_aed, 2)     AS total_spend_aed,
    ROUND(total_spend_usd, 2)     AS total_spend_usd,
    ROUND(avg_transaction_aed, 2) AS avg_transaction_aed,
    ROUND(settled_amount_aed, 2)  AS settled_aed,
    ROUND(refunded_amount_aed, 2) AS refunded_aed,
    declined_count,
    ROUND(100.0 * declined_count / NULLIF(total_transactions + declined_count, 0), 2) AS decline_rate_pct
FROM org_spend
ORDER BY total_spend_aed DESC;
