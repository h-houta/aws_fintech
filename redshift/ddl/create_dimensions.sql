-- =============================================================================
-- Redshift DDL: Dimension Tables
-- Project: Fintech Enterprise AWS Data Pipeline
-- Purpose: Star schema dimensions for corporate card transaction analytics
-- Schema: fintech_enterprise_dw (created in Redshift Serverless namespace)
-- =============================================================================

-- Drop and recreate schema for idempotent deployments
CREATE SCHEMA IF NOT EXISTS fintech_enterprise_dw;
SET search_path TO fintech_enterprise_dw;

-- =============================================================================
-- dim_organizations
-- Source: curated/orgs/ Parquet (via COPY or Spectrum)
-- Grain: one row per organization
-- =============================================================================

DROP TABLE IF EXISTS dim_organizations CASCADE;

CREATE TABLE dim_organizations (
    org_key         BIGINT IDENTITY(1, 1) NOT NULL,  -- surrogate key
    org_id          VARCHAR(64)  NOT NULL,             -- natural key (source id)
    business_name   VARCHAR(255) NOT NULL,
    country         CHAR(2),                           -- ISO-2 country code
    business_type   VARCHAR(100),
    num_employees   INTEGER,
    created_at      TIMESTAMP WITHOUT TIME ZONE,
    updated_at      TIMESTAMP WITHOUT TIME ZONE,
    -- SCD Type 1: overwrite on update
    CONSTRAINT pk_dim_organizations PRIMARY KEY (org_key),
    CONSTRAINT uq_dim_organizations_org_id UNIQUE (org_id)
)
DISTSTYLE ALL  -- small table, broadcast to all nodes
SORTKEY (org_id);


-- =============================================================================
-- dim_users
-- Source: curated/users/ Parquet
-- Grain: one row per cardholder
-- PII: raw email/phone NOT stored — only SHA-256 hashes
-- =============================================================================

DROP TABLE IF EXISTS dim_users CASCADE;

CREATE TABLE dim_users (
    user_key        BIGINT IDENTITY(1, 1) NOT NULL,
    user_id         VARCHAR(64)  NOT NULL,
    name_on_card    VARCHAR(255),
    email_hash      CHAR(64),    -- SHA-256 hex of lowercased email
    phone_hash      CHAR(64),    -- SHA-256 hex of phone_number
    is_active       BOOLEAN,
    is_verified     BOOLEAN,
    created_at      TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT pk_dim_users PRIMARY KEY (user_key),
    CONSTRAINT uq_dim_users_user_id UNIQUE (user_id)
)
DISTSTYLE ALL
SORTKEY (user_id);


-- =============================================================================
-- dim_cards
-- Source: curated/cards/ Parquet
-- Grain: one row per card
-- =============================================================================

DROP TABLE IF EXISTS dim_cards CASCADE;

CREATE TABLE dim_cards (
    card_key            BIGINT IDENTITY(1, 1) NOT NULL,
    card_id             VARCHAR(64)  NOT NULL,
    card_holder_id      VARCHAR(64),
    org_id              VARCHAR(64),
    name_on_card        VARCHAR(255),
    card_type           VARCHAR(20),   -- VIRTUAL / PHYSICAL
    card_usage          VARCHAR(50),
    status              VARCHAR(20),   -- ACTIVE / TERMINATED
    scheme              VARCHAR(20),   -- VISA / MASTERCARD
    card_product_type   VARCHAR(50),
    subtype             VARCHAR(50),
    last_four_digits    CHAR(4),
    issue_date          DATE,
    expiry_date         DATE,
    active_since        DATE,
    allow_atm           BOOLEAN,
    CONSTRAINT pk_dim_cards PRIMARY KEY (card_key),
    CONSTRAINT uq_dim_cards_card_id UNIQUE (card_id)
)
DISTKEY (org_id)
SORTKEY (card_id);


-- =============================================================================
-- dim_merchants
-- Source: derived from transactions (deduplicated)
-- Grain: one row per unique (merchant_name, merchant_city, merchant_country, mcc)
-- =============================================================================

DROP TABLE IF EXISTS dim_merchants CASCADE;

CREATE TABLE dim_merchants (
    merchant_key        BIGINT IDENTITY(1, 1) NOT NULL,
    merchant_name       VARCHAR(255),
    merchant_city       VARCHAR(100),
    merchant_country    CHAR(3),   -- ISO-3 country code from MCC data
    mcc                 CHAR(4),   -- Merchant Category Code
    mcc_description     VARCHAR(255),
    CONSTRAINT pk_dim_merchants PRIMARY KEY (merchant_key)
)
DISTSTYLE ALL
SORTKEY (merchant_name, mcc);


-- =============================================================================
-- dim_dates
-- Source: generated sequence (covers 2020-01-01 → 2030-12-31)
-- Grain: one row per calendar date
-- =============================================================================

DROP TABLE IF EXISTS dim_dates CASCADE;

CREATE TABLE dim_dates (
    date_key        INTEGER NOT NULL,  -- YYYYMMDD integer key
    full_date       DATE    NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(9),
    day_of_month    SMALLINT,
    day_of_week     SMALLINT,          -- 0=Sunday ... 6=Saturday
    day_name        VARCHAR(9),
    week_of_year    SMALLINT,
    is_weekend      BOOLEAN,
    is_month_start  BOOLEAN,
    is_month_end    BOOLEAN,
    CONSTRAINT pk_dim_dates PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);

-- Populate dim_dates for range 2020-2030
-- Uses cross-join number generator (Serverless-compatible, no stl_scan needed)
INSERT INTO dim_dates
WITH
    p0 AS (SELECT 0 AS n UNION ALL SELECT 1),
    p1 AS (SELECT a.n*2 + b.n AS n FROM p0 a CROSS JOIN p0 b),
    p2 AS (SELECT a.n*4 + b.n AS n FROM p1 a CROSS JOIN p1 b),
    p3 AS (SELECT a.n*16 + b.n AS n FROM p2 a CROSS JOIN p2 b),
    p4 AS (SELECT a.n*256 + b.n AS n FROM p3 a CROSS JOIN p3 b),
    nums AS (SELECT n FROM p4 WHERE n <= 4017),
    date_series AS (
        SELECT DATEADD(DAY, n, '2020-01-01'::DATE) AS d FROM nums
        WHERE DATEADD(DAY, n, '2020-01-01'::DATE) <= '2030-12-31'::DATE
    )
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER)    AS date_key,
    d                                           AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT              AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT           AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT             AS month,
    TO_CHAR(d, 'Month')                         AS month_name,
    EXTRACT(DAY FROM d)::SMALLINT               AS day_of_month,
    EXTRACT(DOW FROM d)::SMALLINT               AS day_of_week,
    TO_CHAR(d, 'Day')                           AS day_name,
    EXTRACT(WEEK FROM d)::SMALLINT              AS week_of_year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(DAY FROM d) = 1 THEN TRUE ELSE FALSE END       AS is_month_start,
    CASE WHEN d = LAST_DAY(d) THEN TRUE ELSE FALSE END               AS is_month_end
FROM date_series;
