"""
Glue ETL Job: transform_cards
================================
Business purpose:
  - Read raw cards CSV
  - Cast types (dates, booleans)
  - Uppercase status and card_type for consistency
  - Partition output Parquet by card_type (VIRTUAL / PHYSICAL)

Output: s3://fintech-enterprise-data-lake-curated/cards/card_type=VIRTUAL/ etc.
"""

import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, StringType


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("allow_atm", F.col("allow_atm").cast(BooleanType()))
        .withColumn("issue_date", F.to_date(F.col("issue_date")).cast(DateType()))
        .withColumn("expiry_date", F.to_date(F.col("expiry_date")).cast(DateType()))
        .withColumn("active_since", F.to_date(F.col("active_since")).cast(DateType()))
        .withColumn("created_at", F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        .withColumn("id", F.col("id").cast(StringType()))
        .withColumn("card_holder_id", F.col("card_holder_id").cast(StringType()))
        .withColumn("organization_id", F.col("organization_id").cast(StringType()))
    )


def normalize_enums(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("card_type", F.upper(F.trim(F.col("card_type"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("scheme", F.upper(F.trim(F.col("scheme"))))
        .withColumn("subtype", F.upper(F.trim(F.col("subtype"))))
    )


def run() -> None:
    raw_path = f"s3://{RAW_BUCKET}/cards/"
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    df = cast_types(df)
    df = normalize_enums(df)
    df = df.dropDuplicates(["id"])

    output_path = f"s3://{CURATED_BUCKET}/cards/"
    (
        df.write.mode("overwrite")
        .partitionBy("card_type")
        .parquet(output_path)
    )

    print(f"Wrote {df.count()} cards to {output_path}")


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
