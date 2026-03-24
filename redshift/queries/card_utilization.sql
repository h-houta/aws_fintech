-- =============================================================================
-- Redshift Query: Card Utilization Analysis
-- Business purpose: Understand active vs terminated card ratios by org and
--                   card type — informs product and sales strategy
-- =============================================================================

WITH card_activity AS (
    SELECT
        c.card_key,
        c.card_id,
        c.org_id,
        c.card_type,
        c.status,
        c.issue_date,
        c.expiry_date,
        COUNT(t.transaction_key)                    AS transaction_count,
        COALESCE(SUM(t.billing_amount), 0)          AS total_spend_aed,
        MAX(t.transaction_timestamp)                AS last_used_at
    FROM dim_cards c
    LEFT JOIN fact_transactions t ON c.card_key = t.card_key AND t.status = 'SETTLED'
    GROUP BY c.card_key, c.card_id, c.org_id, c.card_type, c.status, c.issue_date, c.expiry_date
),
org_card_summary AS (
    SELECT
        o.business_name,
        o.country,
        ca.card_type,
        COUNT(*)                                    AS total_cards,
        SUM(CASE WHEN ca.status = 'ACTIVE' THEN 1 ELSE 0 END)     AS active_cards,
        SUM(CASE WHEN ca.status = 'TERMINATED' THEN 1 ELSE 0 END)  AS terminated_cards,
        SUM(CASE WHEN ca.transaction_count > 0 THEN 1 ELSE 0 END)  AS cards_with_transactions,
        SUM(CASE WHEN ca.transaction_count = 0
                  AND ca.status = 'ACTIVE' THEN 1 ELSE 0 END)      AS dormant_active_cards,
        SUM(ca.total_spend_aed)                     AS total_spend_aed,
        AVG(ca.total_spend_aed)                     AS avg_spend_per_card_aed
    FROM card_activity ca
    JOIN dim_organizations o ON ca.org_id = o.org_id
    GROUP BY o.business_name, o.country, ca.card_type
)
SELECT
    business_name,
    country,
    card_type,
    total_cards,
    active_cards,
    terminated_cards,
    cards_with_transactions,
    dormant_active_cards,
    ROUND(100.0 * active_cards / NULLIF(total_cards, 0), 1)              AS active_rate_pct,
    ROUND(100.0 * cards_with_transactions / NULLIF(active_cards, 0), 1)  AS utilization_rate_pct,
    ROUND(100.0 * dormant_active_cards / NULLIF(active_cards, 0), 1)     AS dormancy_rate_pct,
    ROUND(total_spend_aed, 2)                                            AS total_spend_aed,
    ROUND(avg_spend_per_card_aed, 2)                                     AS avg_spend_per_card_aed
FROM org_card_summary
ORDER BY total_cards DESC, card_type;
