"""
Glue ETL Job: transform_transactions
=====================================
Business purpose:
  - Read raw transactions CSV from S3 raw landing zone
  - Normalize all transaction amounts to USD using static FX rates
  - Standardize merchant names and city strings
  - Cast all columns to correct types
  - Partition output Parquet by year/month

Output: s3://fintech-enterprise-data-lake-curated/transactions/year=YYYY/month=MM/
"""

import sys
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, TimestampType

# ── Static FX rates (AED base → USD) ─────────────────────────────────────────────
# Rates as of project snapshot date; replace with live lookup for production

FX_RATES_TO_USD: Dict[str, float] = {
    "AED": 0.2723,
    "USD": 1.0000,
    "EGP": 0.0206,
    "GBP": 1.2700,
    "EUR": 1.0800,
    "SAR": 0.2667,
    "KWD": 3.2500,
}

# ── Merchant name normalization map ──────────────────────────────────────────────

MERCHANT_MAP: Dict[str, str] = {
    "meta platforms, inc.": "Meta",
    "meta platforms inc": "Meta",
    "facebook": "Meta",
    "google llc": "Google",
    "google ireland limited": "Google",
    "alphabet inc": "Google",
    "starbucks coffee": "Starbucks",
    "starbucks corporation": "Starbucks",
    "emirates airline": "Emirates",
    "emirates airlines": "Emirates",
    "b.tech": "B.Tech",
    "b tech": "B.Tech",
    "btech": "B.Tech",
    "amazon web services": "AWS",
    "amazon.com": "Amazon",
}

VALID_CURRENCIES = {"AED", "USD", "EGP", "GBP", "EUR", "SAR", "KWD"}


def normalize_merchant(df: DataFrame) -> DataFrame:
    """Standardize merchant names using a lookup map, fallback to title-case."""
    # Build a SQL CASE expression from the map
    merchant_col = F.lower(F.trim(F.col("merchant")))

    case_expr = F.when(F.lit(False), F.lit(None))  # dummy start
    for raw, clean in MERCHANT_MAP.items():
        case_expr = case_expr.when(merchant_col == raw, clean)
    case_expr = case_expr.otherwise(F.initcap(F.trim(F.col("merchant"))))

    return df.withColumn("merchant_normalized", case_expr)


def normalize_city(df: DataFrame) -> DataFrame:
    """Trim whitespace and title-case merchant cities."""
    return df.withColumn(
        "merchant_city",
        F.initcap(F.trim(F.col("merchant_city"))),
    )


def add_usd_amount(df: DataFrame) -> DataFrame:
    """
    Add usd_amount column: billing_amount converted to USD.
    billing_currency is always AED per data spec.
    """
    fx_map_expr = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in FX_RATES_TO_USD.items()] for item in pair]
    )
    return df.withColumn(
        "usd_amount",
        F.round(
            F.col("billing_amount").cast(DecimalType(18, 6))
            * fx_map_expr[F.col("billing_currency")],
            6,
        ).cast(DecimalType(18, 6)),
    )


def cast_types(df: DataFrame) -> DataFrame:
    """Enforce column types: decimals, timestamps, strings."""
    return (
        df.withColumn("actual_amount", F.col("actual_amount").cast(DecimalType(18, 4)))
        .withColumn("billing_amount", F.col("billing_amount").cast(DecimalType(18, 4)))
        .withColumn("transaction_amount", F.col("transaction_amount").cast(DecimalType(18, 4)))
        .withColumn("fee_amount", F.col("fee_amount").cast(DecimalType(18, 4)))
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(F.col("transaction_timestamp")).cast(TimestampType()),
        )
        .withColumn(
            "clearing_timestamp",
            F.to_timestamp(F.col("clearing_timestamp")).cast(TimestampType()),
        )
        .withColumn("id", F.col("id").cast(StringType()))
        .withColumn("card_id", F.col("card_id").cast(StringType()))
    )


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Derive year/month partition columns from transaction_timestamp."""
    return (
        df.withColumn("year", F.year(F.col("transaction_timestamp")).cast(StringType()))
        .withColumn("month", F.lpad(F.month(F.col("transaction_timestamp")).cast(StringType()), 2, "0"))
    )


def filter_valid_currencies(df: DataFrame) -> DataFrame:
    """Drop rows with unrecognized currency codes (data quality gate)."""
    valid = list(VALID_CURRENCIES)
    return df.filter(F.col("transaction_currency").isin(valid))


def run() -> None:
    # ── Read raw CSV ──────────────────────────────────────────────────────────────
    raw_path = f"s3://{RAW_BUCKET}/transactions/"
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    # ── Transforms ────────────────────────────────────────────────────────────────
    df = cast_types(df)
    df = filter_valid_currencies(df)
    df = normalize_merchant(df)
    df = normalize_city(df)
    df = add_usd_amount(df)
    df = add_partition_columns(df)

    # Drop duplicates on transaction id
    df = df.dropDuplicates(["id"])

    # ── Write curated Parquet ─────────────────────────────────────────────────────
    output_path = f"s3://{CURATED_BUCKET}/transactions/"
    (
        df.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)
    )

    print(f"Wrote {df.count()} transactions to {output_path}")


if __name__ == "__main__":
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "RAW_BUCKET", "CURATED_BUCKET"],
    )

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark: SparkSession = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    RAW_BUCKET = args["RAW_BUCKET"]
    CURATED_BUCKET = args["CURATED_BUCKET"]

    run()
    job.commit()
