"""
Unit tests for Glue ETL transform functions.

Uses chispa for DataFrame equality assertions and a local SparkSession
(no Glue context required — functions are imported directly).

Run: pytest tests/test_transforms.py -v
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from typing import Generator

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Add glue/etl to path for direct import (bypasses Glue context)
sys.path.insert(0, str(Path(__file__).parent.parent / "glue" / "etl"))


# ── Fixtures ──────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("fintech-enterprise-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Transaction transform tests ───────────────────────────────────────────────────

class TestTransformTransactions:
    """Tests for transform_transactions.py functions."""

    def test_merchant_normalization_known_merchant(self, spark: SparkSession) -> None:
        """Known merchant variants should be mapped to canonical names."""
        from transform_transactions import normalize_merchant

        schema = StructType([
            StructField("merchant", StringType(), True),
        ])
        input_data = [
            ("Meta Platforms, Inc.",),
            ("meta platforms inc",),
            ("GOOGLE LLC",),
            ("Starbucks Coffee",),
            ("Unknown Cafe",),
        ]
        df = spark.createDataFrame(input_data, schema)
        result = normalize_merchant(df)

        merchants = [row["merchant_normalized"] for row in result.collect()]
        assert merchants[0] == "Meta"
        assert merchants[1] == "Meta"
        assert merchants[2] == "Google"
        assert merchants[3] == "Starbucks"
        assert merchants[4] == "Unknown Cafe"  # title-cased fallback

    def test_city_normalization_trims_whitespace(self, spark: SparkSession) -> None:
        """Cities with extra whitespace should be trimmed and title-cased."""
        from transform_transactions import normalize_city

        schema = StructType([StructField("merchant_city", StringType(), True)])
        df = spark.createDataFrame([("  dubai  ",), ("CAIRO",), ("london",)], schema)
        result = normalize_city(df)

        cities = [row["merchant_city"] for row in result.collect()]
        assert cities[0] == "Dubai"
        assert cities[1] == "Cairo"
        assert cities[2] == "London"

    def test_usd_conversion_aed(self, spark: SparkSession) -> None:
        """AED billing_amount should be converted to USD using FX rate."""
        from transform_transactions import add_usd_amount

        schema = StructType([
            StructField("billing_amount", StringType(), True),
            StructField("billing_currency", StringType(), True),
        ])
        # 100 AED * 0.2723 = 27.23 USD
        df = spark.createDataFrame([("100.00", "AED")], schema)
        df = df.withColumn("billing_amount", F.col("billing_amount").cast(DecimalType(18, 4)))

        result = add_usd_amount(df)
        usd = result.collect()[0]["usd_amount"]
        assert abs(float(usd) - 27.23) < 0.01

    def test_usd_conversion_usd_passthrough(self, spark: SparkSession) -> None:
        """USD transactions should remain 1:1 (no conversion)."""
        from transform_transactions import add_usd_amount

        schema = StructType([
            StructField("billing_amount", StringType(), True),
            StructField("billing_currency", StringType(), True),
        ])
        df = spark.createDataFrame([("50.00", "USD")], schema)
        df = df.withColumn("billing_amount", F.col("billing_amount").cast(DecimalType(18, 4)))

        result = add_usd_amount(df)
        usd = result.collect()[0]["usd_amount"]
        assert abs(float(usd) - 50.0) < 0.001

    def test_invalid_currencies_filtered(self, spark: SparkSession) -> None:
        """Rows with unrecognized currency codes should be dropped."""
        from transform_transactions import filter_valid_currencies

        schema = StructType([StructField("transaction_currency", StringType(), True)])
        df = spark.createDataFrame(
            [("AED",), ("USD",), ("XYZ",), ("INVALID",), ("GBP",)], schema
        )
        result = filter_valid_currencies(df)
        currencies = {row["transaction_currency"] for row in result.collect()}
        assert "XYZ" not in currencies
        assert "INVALID" not in currencies
        assert "AED" in currencies
        assert "GBP" in currencies

    def test_partition_columns_added(self, spark: SparkSession) -> None:
        """Year and month partition columns should be derived from transaction_timestamp."""
        from transform_transactions import add_partition_columns

        schema = StructType([
            StructField("transaction_timestamp", TimestampType(), True),
        ])
        from datetime import datetime

        df = spark.createDataFrame([(datetime(2024, 3, 15, 10, 30),)], schema)
        result = add_partition_columns(df)
        row = result.collect()[0]
        assert row["year"] == "2024"
        assert row["month"] == "03"


# ── Users transform tests ─────────────────────────────────────────────────────────

class TestTransformUsers:
    """Tests for transform_users.py functions."""

    def test_email_pii_removed(self, spark: SparkSession) -> None:
        """Raw email column should be dropped and email_hash added."""
        from transform_users import mask_pii

        schema = StructType([
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("password", StringType(), True),
            StructField("id", StringType(), True),
        ])
        df = spark.createDataFrame([("user@example.com", "+971501234567", "hashed_pw", "1")], schema)
        result = mask_pii(df)

        col_names = result.columns
        assert "email" not in col_names
        assert "phone_number" not in col_names
        assert "password" not in col_names
        assert "email_hash" in col_names
        assert "phone_hash" in col_names

    def test_email_hash_deterministic(self, spark: SparkSession) -> None:
        """Same email input should always produce the same SHA-256 hash."""
        from transform_users import mask_pii

        schema = StructType([
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("password", StringType(), True),
            StructField("id", StringType(), True),
        ])
        df1 = spark.createDataFrame([("test@fintech-enterprise.io", "+1234", "pw", "1")], schema)
        df2 = spark.createDataFrame([("test@fintech-enterprise.io", "+1234", "pw", "1")], schema)

        hash1 = mask_pii(df1).collect()[0]["email_hash"]
        hash2 = mask_pii(df2).collect()[0]["email_hash"]
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex = 64 chars

    def test_email_hash_case_insensitive(self, spark: SparkSession) -> None:
        """Email hashing should be case-insensitive (lowercased before hashing)."""
        from transform_users import mask_pii

        schema = StructType([
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("password", StringType(), True),
            StructField("id", StringType(), True),
        ])
        df_lower = spark.createDataFrame([("user@fintech-enterprise.io", "+1", "pw", "1")], schema)
        df_upper = spark.createDataFrame([("USER@FINTECH_ENTERPRISE.IO", "+1", "pw", "1")], schema)

        hash_lower = mask_pii(df_lower).collect()[0]["email_hash"]
        hash_upper = mask_pii(df_upper).collect()[0]["email_hash"]
        assert hash_lower == hash_upper


# ── Cards transform tests ─────────────────────────────────────────────────────────

class TestTransformCards:
    """Tests for transform_cards.py functions."""

    def test_card_type_uppercased(self, spark: SparkSession) -> None:
        """card_type and status should be uppercased and trimmed."""
        from transform_cards import normalize_enums

        schema = StructType([
            StructField("card_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("scheme", StringType(), True),
            StructField("subtype", StringType(), True),
        ])
        df = spark.createDataFrame([("  virtual  ", "active", "visa", "single")], schema)
        result = normalize_enums(df)
        row = result.collect()[0]

        assert row["card_type"] == "VIRTUAL"
        assert row["status"] == "ACTIVE"
        assert row["scheme"] == "VISA"
