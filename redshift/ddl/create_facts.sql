-- =============================================================================
-- Redshift DDL: Fact Tables
-- Project: Fintech Enterprise AWS Data Pipeline
-- Purpose: Transaction and clearing facts for corporate card analytics
-- Prerequisites: run create_dimensions.sql first
-- =============================================================================

SET search_path TO fintech_enterprise_dw;

-- =============================================================================
-- fact_transactions
-- Source: curated/transactions/ Parquet
-- Grain: one row per card transaction
-- =============================================================================

DROP TABLE IF EXISTS fact_transactions CASCADE;

CREATE TABLE fact_transactions (
    transaction_key     BIGINT IDENTITY(1, 1) NOT NULL,
    transaction_id      VARCHAR(64)   NOT NULL,         -- natural key
    card_key            BIGINT        NOT NULL,          -- FK → dim_cards
    org_key             BIGINT,                          -- FK → dim_organizations (via card)
    merchant_key        BIGINT,                          -- FK → dim_merchants
    date_key            INTEGER       NOT NULL,          -- FK → dim_dates (YYYYMMDD)

    -- Amounts
    actual_amount       DECIMAL(18, 4),                  -- in actual_currency
    actual_currency     CHAR(3),
    billing_amount      DECIMAL(18, 4),                  -- in AED (billing)
    billing_currency    CHAR(3)       DEFAULT 'AED',
    transaction_amount  DECIMAL(18, 4),                  -- in transaction_currency
    transaction_currency CHAR(3),
    usd_amount          DECIMAL(18, 6),                  -- derived: billing_amount * FX → USD
    fee_amount          DECIMAL(18, 4),
    fee_currency        CHAR(3),

    -- Transaction attributes
    status              VARCHAR(20),   -- SETTLED / REFUNDED / DECLINED
    dr_cr               VARCHAR(10),   -- DEBIT / CREDIT
    type                VARCHAR(50),
    mcc                 CHAR(4),

    -- Merchant denormalized (for fast queries without join)
    merchant            VARCHAR(255),
    merchant_city       VARCHAR(100),
    merchant_country    CHAR(3),

    -- Timestamps
    transaction_timestamp   TIMESTAMP WITHOUT TIME ZONE,
    clearing_timestamp      TIMESTAMP WITHOUT TIME ZONE,

    CONSTRAINT pk_fact_transactions PRIMARY KEY (transaction_key),
    CONSTRAINT uq_fact_transactions_id UNIQUE (transaction_id),
    CONSTRAINT fk_ft_card   FOREIGN KEY (card_key)     REFERENCES dim_cards (card_key),
    CONSTRAINT fk_ft_org    FOREIGN KEY (org_key)      REFERENCES dim_organizations (org_key),
    CONSTRAINT fk_ft_merch  FOREIGN KEY (merchant_key) REFERENCES dim_merchants (merchant_key),
    CONSTRAINT fk_ft_date   FOREIGN KEY (date_key)     REFERENCES dim_dates (date_key)
)
DISTKEY (card_key)                  -- collocate with dim_cards for join performance
COMPOUND SORTKEY (date_key, status);


-- =============================================================================
-- fact_clearing
-- Source: curated/clearing/ Parquet (Mastercard network settlement data)
-- Grain: one row per clearing record
-- =============================================================================

DROP TABLE IF EXISTS fact_clearing CASCADE;

CREATE TABLE fact_clearing (
    clearing_key                BIGINT IDENTITY(1, 1) NOT NULL,
    clearing_id                 VARCHAR(64)   NOT NULL,
    transaction_id              VARCHAR(64),                    -- FK to fact_transactions (natural key)
    card_key                    BIGINT,                         -- FK → dim_cards
    date_key                    INTEGER,                        -- FK → dim_dates (capture date)

    -- Amounts
    transaction_amount          DECIMAL(18, 6),
    transaction_currency        CHAR(3),
    gtv                         DECIMAL(18, 6),                 -- Gross Transaction Value
    interchange_recon_fee       DECIMAL(18, 6),
    interchange_fee_sign        VARCHAR(10),                    -- DR / CR

    -- Settlement attributes
    settlement_status           VARCHAR(50),
    network                     VARCHAR(50),                    -- MASTERCARD
    transaction_type            VARCHAR(50),
    mcc                         CHAR(4),
    mti                         VARCHAR(10),                    -- Message Type Indicator
    approval_code               VARCHAR(20),

    -- Merchant
    merchant_name               VARCHAR(255),
    merchant_city               VARCHAR(100),
    merchant_country            CHAR(3),

    -- Dates
    transaction_timestamp_utc   TIMESTAMP WITHOUT TIME ZONE,
    capture_date_utc            TIMESTAMP WITHOUT TIME ZONE,
    settlement_date_utc         TIMESTAMP WITHOUT TIME ZONE,

    CONSTRAINT pk_fact_clearing PRIMARY KEY (clearing_key),
    CONSTRAINT uq_fact_clearing_id UNIQUE (clearing_id),
    CONSTRAINT fk_fc_card FOREIGN KEY (card_key) REFERENCES dim_cards (card_key),
    CONSTRAINT fk_fc_date FOREIGN KEY (date_key) REFERENCES dim_dates (date_key)
)
DISTKEY (card_key)
COMPOUND SORTKEY (date_key, settlement_status);
