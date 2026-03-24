"""
Glue ETL Job: transform_clearing
==================================
Business purpose:
  - Read raw clearing CSV (90+ columns, Mastercard network data)
  - Normalize all timestamps to UTC (clearing data has mixed timezone formats)
  - Cast interchange fee and GTV amounts to Decimal
  - Standardize settlement_status strings
  - Partition output Parquet by period (YYYY-MM from capture_date)

Output: s3://fintech-enterprise-data-lake-curated/clearing/period=YYYY-MM/
"""

import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, TimestampType

# Known timestamp patterns in clearing data
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd'T'HH:mm:ssZ",
    "dd/MM/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
]


def parse_timestamp_utc(col_name: str, df: DataFrame) -> DataFrame:
    """
    Attempt multiple timestamp formats and convert to UTC.
    Falls back to null if none match — null rows are flagged for DQ.
    """
    parsed = F.lit(None).cast(TimestampType())
    for fmt in TIMESTAMP_FORMATS:
        parsed = F.when(parsed.isNull(), F.to_utc_timestamp(F.to_timestamp(F.col(col_name), fmt), "UTC")).otherwise(parsed)

    return df.withColumn(col_name + "_utc", parsed)


def normalize_timestamps(df: DataFrame) -> DataFrame:
    """Normalize all timestamp columns to UTC."""
    for col_name in ["transaction_timestamp", "capture_date", "settlement_date"]:
        if col_name in df.columns:
            df = parse_timestamp_utc(col_name, df)
    return df


def cast_financial_columns(df: DataFrame) -> DataFrame:
    """Cast monetary and fee columns to Decimal(18,6)."""
    financial_cols = [
        "transaction_amount",
        "interchange_recon_fee_amount",
        "gtv",
    ]
    for col in financial_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DecimalType(18, 6)))
    return df


def normalize_settlement_status(df: DataFrame) -> DataFrame:
    """Uppercase and trim settlement_status values."""
    if "settlement_status" in df.columns:
        df = df.withColumn(
            "settlement_status",
            F.upper(F.trim(F.col("settlement_status"))),
        )
    return df


def add_period_partition(df: DataFrame) -> DataFrame:
    """
    Derive period column (YYYY-MM) from capture_date for partitioning.
    Falls back to period column if already present.
    """
    if "period" in df.columns:
        # Ensure YYYY-MM format
        df = df.withColumn(
            "period",
            F.regexp_replace(F.col("period").cast(StringType()), r"^(\d{4}-\d{2}).*$", "$1"),
        )
    elif "capture_date_utc" in df.columns:
        df = df.withColumn(
            "period",
            F.date_format(F.col("capture_date_utc"), "yyyy-MM"),
        )
    else:
        df = df.withColumn("period", F.lit("unknown"))
    return df


def rename_columns(df: DataFrame) -> DataFrame:
    """Rename all columns to snake_case (lowercase, spaces to underscores)."""
    import re
    for col_name in df.columns:
        snake = re.sub(r'\s+', '_', col_name.strip()).lower()
        # Remove consecutive underscores
        snake = re.sub(r'_+', '_', snake)
        if snake != col_name:
            df = df.withColumnRenamed(col_name, snake)
    return df


def run() -> None:
    raw_path = f"s3://{RAW_BUCKET}/clearing/"
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    df = rename_columns(df)
    df = normalize_timestamps(df)
    df = cast_financial_columns(df)
    df = normalize_settlement_status(df)
    df = add_period_partition(df)
    df = df.dropDuplicates(["transaction_id"])

    output_path = f"s3://{CURATED_BUCKET}/clearing/"
    (
        df.write.mode("overwrite")
        .partitionBy("period")
        .parquet(output_path)
    )

    print(f"Wrote {df.count()} clearing records to {output_path}")


if __name__ == "__main__":
    from awsglue.context import GlueContext
    from awsglue.job import Job
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
