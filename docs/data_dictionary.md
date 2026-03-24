# Data Dictionary — Fintech Enterprise AWS Data Pipeline

## Source Tables (Raw Layer)

### orgs
| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Primary key — organization identifier |
| business_name | STRING | Registered company name |
| contact_number | STRING | Business phone (sparse) |
| business_type | STRING | Industry category |
| invoices_per_month | INTEGER | Estimated monthly invoices |
| number_of_employees | INTEGER | Headcount |
| kyb_id | STRING | Know-Your-Business verification ID |
| track_id | STRING | Internal tracking reference |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Last update time |
| deleted_at | TIMESTAMP | Soft-delete timestamp (null = active) |
| country | STRING | ISO-2 country code |

### users
| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Primary key — user identifier |
| first_name | STRING | **PII — anonymized in curated layer** |
| last_name | STRING | **PII — anonymized in curated layer** |
| name_on_card | STRING | Embossed card name |
| email | STRING | **PII — SHA-256 hashed in curated layer** |
| phone_number | STRING | **PII — SHA-256 hashed in curated layer** |
| is_active | BOOLEAN | Account active flag |
| is_verified | BOOLEAN | KYC verified flag |
| password | STRING | **Redacted entirely in curated layer** |
| track_id | STRING | Internal tracking reference |
| created_at | TIMESTAMP | Registration timestamp |

### cards
| Column | Type | Description |
|--------|------|-------------|
| card_id | STRING | Primary key — alphanumeric hash (e.g., XENDOSw_4YFR3IA5GUqGN) |
| card_holder_id | STRING | FK → users.id |
| card_issuer_id | STRING | Issuing entity ID |
| organization_id | STRING | FK → orgs.id |
| card_type | STRING | VIRTUAL or PHYSICAL |
| status | STRING | ACTIVE or TERMINATED |
| last_four_digits | STRING | Last 4 digits of card PAN |
| scheme | STRING | VISA or MASTERCARD |
| issue_date | DATE | Card issuance date |
| expiry_date | DATE | Card expiry date |
| allow_atm | BOOLEAN | ATM withdrawal permitted |

### transactions
| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Primary key — transaction ID |
| card_id | STRING | FK → cards.card_id |
| actual_amount | DECIMAL(18,4) | Amount in actual_currency |
| actual_currency | STRING | Currency of actual_amount (AED) |
| billing_amount | DECIMAL(18,4) | Amount billed in AED |
| billing_currency | STRING | Always AED for Fintech Enterprise |
| transaction_amount | DECIMAL(18,4) | Amount in transaction_currency |
| transaction_currency | STRING | Original transaction currency (AED/USD/EGP/GBP/EUR) |
| status | STRING | SETTLED / REFUNDED / DECLINED / PENDING |
| dr_cr | STRING | DEBIT or CREDIT |
| fee_amount | DECIMAL(18,4) | Transaction fee |
| mcc | STRING | 4-digit Merchant Category Code |
| merchant | STRING | Merchant name (raw — normalized in ETL) |
| merchant_city | STRING | Merchant city (trimmed in ETL) |
| merchant_country | STRING | ISO-3 merchant country |
| transaction_timestamp | TIMESTAMP | When transaction occurred |
| clearing_timestamp | TIMESTAMP | When transaction cleared |

### clearing
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | STRING | FK → transactions.id |
| network | STRING | Always MASTERCARD |
| mti | STRING | ISO 8583 Message Type Indicator |
| transaction_code | STRING | Network transaction code |
| merchant_name | STRING | Merchant name from network |
| mcc | STRING | Merchant Category Code |
| transaction_amount | DECIMAL(18,6) | Original transaction amount |
| transaction_currency | STRING | Transaction currency |
| GTV | DECIMAL(18,6) | Gross Transaction Value |
| interchange_recon_fee_amount | DECIMAL(18,6) | Interchange fee (reconciliation) |
| interchange_fee_sign | STRING | DR (debit) or CR (credit) |
| settlement_status | STRING | Settlement state |
| capture_date | TIMESTAMP | Network capture timestamp |
| settlement_date | TIMESTAMP | Final settlement timestamp |

---

## Curated Layer Changes

| Table | PII Action | Notes |
|-------|-----------|-------|
| users | email → email_hash (SHA-256), phone_number → phone_hash, password DROPPED | Hash is deterministic for cross-dataset joins |
| transactions | merchant name normalized, city trimmed, usd_amount added | FX rate applied per static rates table |
| clearing | timestamps normalized to UTC, column names snake_cased | Mixed timezone formats handled in ETL |
| cards | card_type/status uppercased, dates cast to DATE | Partitioned by card_type |

---

## Dimensional Model (Redshift fintech_enterprise_dw)

| Table | Type | Grain | Distribution |
|-------|------|-------|-------------|
| dim_organizations | Dimension | 1 row/org | DISTSTYLE ALL |
| dim_users | Dimension | 1 row/user | DISTSTYLE ALL |
| dim_cards | Dimension | 1 row/card | DISTKEY(org_id) |
| dim_merchants | Dimension | 1 row/unique merchant+city+mcc | DISTSTYLE ALL |
| dim_dates | Dimension | 1 row/calendar date (2020–2030) | DISTSTYLE ALL |
| fact_transactions | Fact | 1 row/transaction | DISTKEY(card_key) |
| fact_clearing | Fact | 1 row/clearing record | DISTKEY(card_key) |
