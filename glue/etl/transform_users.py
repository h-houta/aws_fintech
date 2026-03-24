"""
Glue ETL Job: transform_users
================================
Business purpose:
  - Read raw users CSV
  - Hash PII fields (email, phone_number) with SHA-256 for curated layer
  - Drop plaintext password column entirely
  - Cast types
  - Write unpartitioned Parquet (small table, ~20 rows)

Output: s3://fintech-enterprise-data-lake-curated/users/
"""

import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, TimestampType


def mask_pii(df: DataFrame) -> DataFrame:
    """
    Replace PII with SHA-256 hashes.
    SHA-256 is deterministic (same input → same hash) which preserves
    the ability to join on email_hash across datasets if needed.
    """
    return (
        df.withColumn("email_hash", F.sha2(F.lower(F.trim(F.col("email"))), 256))
        .withColumn("phone_hash", F.sha2(F.trim(F.col("phone_number")), 256))
        .drop("email", "phone_number", "password")  # Remove raw PII entirely
    )


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("id", F.col("id").cast(StringType()))
        .withColumn("is_active", F.col("is_active").cast(BooleanType()))
        .withColumn("is_verified", F.col("is_verified").cast(BooleanType()))
        .withColumn("created_at", F.to_timestamp(F.col("created_at")).cast(TimestampType()))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")).cast(TimestampType()))
    )


def run() -> None:
    raw_path = f"s3://{RAW_BUCKET}/users/"
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    df = cast_types(df)
    df = mask_pii(df)
    df = df.dropDuplicates(["id"])

    output_path = f"s3://{CURATED_BUCKET}/users/"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Wrote {df.count()} users to {output_path} (PII masked)")


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
