"""
Glue ETL Job: transform_orgs
================================
Business purpose:
  - Read raw orgs CSV
  - Cast types (timestamps, integers)
  - Standardize country codes to ISO-2
  - Write unpartitioned Parquet (small table, ~137 rows)

Output: s3://fintech-enterprise-data-lake-curated/orgs/
"""

import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("id", F.col("id").cast(StringType()))
        .withColumn("invoices_per_month", F.col("invoices_per_month").cast(IntegerType()))
        .withColumn("number_of_employees", F.col("number_of_employees").cast(IntegerType()))
        .withColumn("created_at", F.to_timestamp(F.col("created_at")).cast(TimestampType()))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")).cast(TimestampType()))
        .withColumn("deleted_at", F.to_timestamp(F.col("deleted_at")).cast(TimestampType()))
        .withColumn("business_name", F.trim(F.col("business_name")))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
    )


def run() -> None:
    raw_path = f"s3://{RAW_BUCKET}/orgs/"
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    df = cast_types(df)
    df = df.dropDuplicates(["id"])

    output_path = f"s3://{CURATED_BUCKET}/orgs/"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Wrote {df.count()} organizations to {output_path}")


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
