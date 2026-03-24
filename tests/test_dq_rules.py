"""
Unit tests for Data Quality rule logic.

These tests validate the DQ constraints defined in data_quality/glue_dq_rules.json
by running equivalent checks against fixture DataFrames using a local SparkSession.

Run: pytest tests/test_dq_rules.py -v
"""

from __future__ import annotations

from typing import Generator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("fintech-enterprise-dq-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


VALID_CURRENCIES = {"AED", "USD", "EGP", "GBP", "EUR", "SAR", "KWD"}
VALID_DR_CR = {"DEBIT", "CREDIT"}
VALID_CARD_STATUSES = {"ACTIVE", "TERMINATED"}


class TestTransactionDQRules:
    """DQ rules for the transactions table."""

    def test_no_null_card_ids(self, spark: SparkSession) -> None:
        """card_id must never be null."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("card_id", StringType(), True),
        ])
        df = spark.createDataFrame(
            [("txn1", "CARD-001"), ("txn2", None), ("txn3", "CARD-002")],
            schema,
        )
        null_count = df.filter(F.col("card_id").isNull()).count()
        assert null_count == 1  # fixture has 1 null — rule would fail in prod

    def test_settled_positive_amount(self, spark: SparkSession) -> None:
        """Settled transactions must have billing_amount > 0."""
        schema = StructType([
            StructField("status", StringType(), True),
            StructField("billing_amount", DecimalType(18, 4), True),
        ])
        from decimal import Decimal

        df = spark.createDataFrame(
            [
                ("SETTLED", Decimal("100.00")),
                ("SETTLED", Decimal("-5.00")),   # VIOLATION
                ("DECLINED", Decimal("0.00")),   # OK — not SETTLED
                ("SETTLED", Decimal("0.00")),    # VIOLATION
            ],
            schema,
        )
        violations = df.filter(
            (F.col("status") == "SETTLED") & (F.col("billing_amount") <= 0)
        ).count()
        assert violations == 2

    def test_valid_currency_codes(self, spark: SparkSession) -> None:
        """transaction_currency must be one of the approved codes."""
        schema = StructType([StructField("transaction_currency", StringType(), True)])
        df = spark.createDataFrame(
            [("AED",), ("USD",), ("XYZ",), ("BITCOIN",), ("EUR",)],
            schema,
        )
        invalid = df.filter(~F.col("transaction_currency").isin(list(VALID_CURRENCIES))).count()
        assert invalid == 2  # XYZ and BITCOIN

    def test_no_duplicate_transaction_ids(self, spark: SparkSession) -> None:
        """Transaction IDs must be unique."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame(
            [("txn-001",), ("txn-002",), ("txn-001",), ("txn-003",)],
            schema,
        )
        total = df.count()
        distinct = df.dropDuplicates(["id"]).count()
        duplicates = total - distinct
        assert duplicates == 1

    def test_valid_dr_cr_values(self, spark: SparkSession) -> None:
        """dr_cr must be DEBIT or CREDIT only."""
        schema = StructType([StructField("dr_cr", StringType(), True)])
        df = spark.createDataFrame(
            [("DEBIT",), ("CREDIT",), ("UNKNOWN",), ("debit",)],
            schema,
        )
        invalid = df.filter(~F.col("dr_cr").isin(list(VALID_DR_CR))).count()
        assert invalid == 2  # UNKNOWN and lowercase 'debit'

    def test_referential_integrity_card_ids(self, spark: SparkSession) -> None:
        """Every card_id in transactions must exist in cards table."""
        txn_schema = StructType([StructField("card_id", StringType(), True)])
        card_schema = StructType([StructField("card_id", StringType(), True)])

        transactions = spark.createDataFrame(
            [("CARD-001",), ("CARD-002",), ("CARD-999",)],  # CARD-999 is orphaned
            txn_schema,
        )
        cards = spark.createDataFrame(
            [("CARD-001",), ("CARD-002",), ("CARD-003",)],
            card_schema,
        )

        orphaned = transactions.join(cards, on="card_id", how="left_anti").count()
        assert orphaned == 1  # CARD-999 not in cards


class TestClearingDQRules:
    """DQ rules for the clearing table."""

    def test_settlement_after_transaction(self, spark: SparkSession) -> None:
        """Settlement date must be >= transaction timestamp."""
        from datetime import datetime

        schema = StructType([
            StructField("transaction_timestamp", TimestampType(), True),
            StructField("settlement_date", TimestampType(), True),
        ])
        df = spark.createDataFrame(
            [
                (datetime(2024, 1, 1), datetime(2024, 1, 2)),   # OK
                (datetime(2024, 1, 5), datetime(2024, 1, 3)),   # VIOLATION: settle before txn
                (datetime(2024, 1, 1), datetime(2024, 1, 1)),   # OK: same day
            ],
            schema,
        )
        violations = df.filter(
            F.col("settlement_date") < F.col("transaction_timestamp")
        ).count()
        assert violations == 1

    def test_no_duplicate_clearing_transaction_ids(self, spark: SparkSession) -> None:
        """transaction_id in clearing must be unique."""
        schema = StructType([StructField("transaction_id", StringType(), True)])
        df = spark.createDataFrame(
            [("TXN-001",), ("TXN-002",), ("TXN-001",)],
            schema,
        )
        duplicates = df.count() - df.dropDuplicates(["transaction_id"]).count()
        assert duplicates == 1


class TestCardsDQRules:
    """DQ rules for the cards table."""

    def test_valid_card_status(self, spark: SparkSession) -> None:
        """Card status must be ACTIVE or TERMINATED."""
        schema = StructType([StructField("status", StringType(), True)])
        df = spark.createDataFrame(
            [("ACTIVE",), ("TERMINATED",), ("PENDING",), ("FROZEN",)],
            schema,
        )
        invalid = df.filter(~F.col("status").isin(list(VALID_CARD_STATUSES))).count()
        assert invalid == 2  # PENDING and FROZEN
