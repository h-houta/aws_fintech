-- =============================================================================
-- Redshift COPY Commands: Load curated Parquet → Redshift tables
-- Project: Fintech Enterprise AWS Data Pipeline
-- Purpose: Bulk load dimension and fact tables from curated S3 Parquet
-- Prerequisites:
--   - Dimensions loaded before facts (FK constraints)
--   - Replace {ACCOUNT_ID} and {REGION} placeholders
--   - Replace {CURATED_BUCKET} with actual bucket name
-- =============================================================================

SET search_path TO fintech_enterprise_dw;

-- ── dim_organizations ────────────────────────────────────────────────────────────

COPY dim_organizations (org_id, business_name, country, business_type, num_employees, created_at, updated_at)
FROM 's3://{CURATED_BUCKET}/orgs/'
IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/fintech-enterprise-redshift-role'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── dim_users ────────────────────────────────────────────────────────────────────

COPY dim_users (user_id, name_on_card, email_hash, phone_hash, is_active, is_verified, created_at)
FROM 's3://{CURATED_BUCKET}/users/'
IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/fintech-enterprise-redshift-role'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── dim_cards ────────────────────────────────────────────────────────────────────

COPY dim_cards (
    card_id, card_holder_id, org_id, name_on_card, card_type, card_usage,
    status, scheme, card_product_type, subtype, last_four_digits,
    issue_date, expiry_date, active_since, allow_atm
)
FROM 's3://{CURATED_BUCKET}/cards/'
IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/fintech-enterprise-redshift-role'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── dim_merchants (derived from transactions) ─────────────────────────────────────

INSERT INTO dim_merchants (merchant_name, merchant_city, merchant_country, mcc)
SELECT DISTINCT
    merchant_normalized AS merchant_name,
    merchant_city,
    merchant_country,
    mcc
FROM fintech_enterprise_spectrum.transactions
WHERE merchant_normalized IS NOT NULL
ON CONFLICT DO NOTHING;

-- ── fact_transactions ────────────────────────────────────────────────────────────
-- Loads via intermediate staging table to resolve surrogate key lookups

CREATE TEMP TABLE stg_transactions (LIKE fact_transactions) DISTKEY (card_key);

COPY stg_transactions (
    transaction_id, actual_amount, actual_currency, billing_amount, billing_currency,
    transaction_amount, transaction_currency, usd_amount, fee_amount, fee_currency,
    status, dr_cr, type, mcc, merchant, merchant_city, merchant_country,
    transaction_timestamp, clearing_timestamp
)
FROM 's3://{CURATED_BUCKET}/transactions/'
IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/fintech-enterprise-redshift-role'
FORMAT AS PARQUET
SERIALIZETOJSON;

INSERT INTO fact_transactions (
    transaction_id, card_key, org_key, merchant_key, date_key,
    actual_amount, actual_currency, billing_amount, billing_currency,
    transaction_amount, transaction_currency, usd_amount, fee_amount, fee_currency,
    status, dr_cr, type, mcc, merchant, merchant_city, merchant_country,
    transaction_timestamp, clearing_timestamp
)
SELECT
    s.transaction_id,
    c.card_key,
    o.org_key,
    m.merchant_key,
    CAST(TO_CHAR(s.transaction_timestamp::DATE, 'YYYYMMDD') AS INTEGER) AS date_key,
    s.actual_amount, s.actual_currency,
    s.billing_amount, s.billing_currency,
    s.transaction_amount, s.transaction_currency,
    s.usd_amount,
    s.fee_amount, s.fee_currency,
    s.status, s.dr_cr, s.type, s.mcc,
    s.merchant, s.merchant_city, s.merchant_country,
    s.transaction_timestamp, s.clearing_timestamp
FROM stg_transactions s
LEFT JOIN dim_cards c ON s.transaction_id IS NOT NULL  -- join via card_id (from stg or spectrum)
LEFT JOIN dim_organizations o ON c.org_id = o.org_id
LEFT JOIN dim_merchants m
    ON s.merchant = m.merchant_name
    AND COALESCE(s.merchant_city, '') = COALESCE(m.merchant_city, '')
    AND COALESCE(s.mcc, '') = COALESCE(m.mcc, '');

DROP TABLE stg_transactions;

-- ── fact_clearing ─────────────────────────────────────────────────────────────────

COPY fact_clearing (
    clearing_id, transaction_id, transaction_amount, transaction_currency,
    gtv, interchange_recon_fee, interchange_fee_sign,
    settlement_status, network, transaction_type, mcc, mti,
    merchant_name, merchant_city, merchant_country,
    transaction_timestamp_utc, capture_date_utc, settlement_date_utc
)
FROM 's3://{CURATED_BUCKET}/clearing/'
IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/fintech-enterprise-redshift-role'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── VACUUM & ANALYZE after bulk load ─────────────────────────────────────────────

VACUUM dim_organizations;
VACUUM dim_cards;
VACUUM fact_transactions;
VACUUM fact_clearing;

ANALYZE dim_organizations;
ANALYZE dim_cards;
ANALYZE fact_transactions;
ANALYZE fact_clearing;
