"""
Airflow DAG: fintech_enterprise_aws_pipeline
================================
End-to-end orchestration for the Fintech Enterprise AWS data engineering pipeline.

Task flow:
  check_s3_raw
      ↓
  run_glue_crawler
      ↓
  wait_crawler_complete
      ↓
  run_glue_etl (fan-out: 5 parallel ETL jobs)
      ↓
  run_dq_checks
      ↓
  load_redshift
      ↓
  notify_success

Schedule: daily at 02:00 UTC (after overnight batch processing)
"""

from __future__ import annotations

import os
import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ── Default args ──────────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "fintech-enterprise-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,  # Using SNS instead
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

# ── Config ────────────────────────────────────────────────────────────────────────

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
RAW_BUCKET = os.getenv("RAW_BUCKET", "fintech-enterprise-data-lake-raw")
CURATED_BUCKET = os.getenv("CURATED_BUCKET", "fintech-enterprise-data-lake-curated")
CRAWLER_NAME = os.getenv("GLUE_CRAWLER_NAME", "fintech-enterprise-raw-crawler")
SNS_TOPIC_ARN = os.getenv("SNS_ALERT_TOPIC_ARN", "")

GLUE_JOBS = {
    "transactions": os.getenv("GLUE_JOB_TRANSACTIONS", "fintech-enterprise-transform-transactions"),
    "clearing": os.getenv("GLUE_JOB_CLEARING", "fintech-enterprise-transform-clearing"),
    "cards": os.getenv("GLUE_JOB_CARDS", "fintech-enterprise-transform-cards"),
    "users": os.getenv("GLUE_JOB_USERS", "fintech-enterprise-transform-users"),
    "orgs": os.getenv("GLUE_JOB_ORGS", "fintech-enterprise-transform-orgs"),
}

REDSHIFT_WORKGROUP = os.getenv("REDSHIFT_WORKGROUP", "fintech-enterprise-wg")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "fintech_enterprise_dw")

# Demo mode: tasks simulate success with realistic delays instead of calling AWS
DEMO_MODE = os.getenv("AIRFLOW_DEMO_MODE", "true").lower() == "true"


# ── Task functions ────────────────────────────────────────────────────────────────

def check_s3_raw(**context: dict) -> dict:
    """Verify raw source files exist in S3 before triggering pipeline."""
    expected_prefixes = ["orgs/", "users/", "cards/", "transactions/", "clearing/"]

    if DEMO_MODE:
        time.sleep(random.uniform(1, 3))
        print(f"[demo] All source prefixes present in s3://{RAW_BUCKET}/")
        return {"checked_prefixes": expected_prefixes}

    import boto3
    s3 = boto3.client("s3", region_name=AWS_REGION)
    missing = []

    for prefix in expected_prefixes:
        resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            missing.append(prefix)

    if missing:
        raise ValueError(f"Missing raw files for prefixes: {missing}")

    print(f"All source prefixes present in s3://{RAW_BUCKET}/")
    return {"checked_prefixes": expected_prefixes}


def run_glue_crawler(**context: dict) -> str:
    """Start the Glue crawler and return its name."""
    if DEMO_MODE:
        time.sleep(random.uniform(1, 3))
        print(f"[demo] Started crawler: {CRAWLER_NAME}")
        return CRAWLER_NAME

    import boto3
    glue = boto3.client("glue", region_name=AWS_REGION)

    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        print(f"Started crawler: {CRAWLER_NAME}")
    except glue.exceptions.CrawlerRunningException:
        print(f"Crawler {CRAWLER_NAME} already running")

    return CRAWLER_NAME


def wait_crawler_complete(**context: dict) -> None:
    """Poll crawler state until READY. Raises on failure."""
    if DEMO_MODE:
        time.sleep(random.uniform(2, 5))
        print(f"[demo] Crawler {CRAWLER_NAME} completed successfully")
        return

    import boto3
    glue = boto3.client("glue", region_name=AWS_REGION)
    max_wait = 1800
    poll_interval = 20
    elapsed = 0

    while elapsed < max_wait:
        resp = glue.get_crawler(Name=CRAWLER_NAME)
        state = resp["Crawler"]["State"]
        print(f"Crawler state: {state} ({elapsed}s elapsed)")

        if state == "READY":
            last_crawl = resp["Crawler"].get("LastCrawl", {})
            status = last_crawl.get("Status", "UNKNOWN")
            if status == "FAILED":
                raise RuntimeError(f"Crawler failed: {last_crawl.get('ErrorMessage', 'unknown error')}")
            print(f"Crawler completed successfully")
            return

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Crawler {CRAWLER_NAME} timed out after {max_wait}s")


def _run_glue_job(job_name: str, **context: dict) -> str:
    """Start a single Glue ETL job and return its run ID."""
    if DEMO_MODE:
        time.sleep(random.uniform(2, 6))
        run_id = f"jr_demo_{random.randint(100000, 999999)}"
        print(f"[demo] Glue job {job_name} SUCCEEDED (run_id={run_id})")
        return run_id

    import boto3
    glue = boto3.client("glue", region_name=AWS_REGION)

    resp = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--RAW_BUCKET": RAW_BUCKET,
            "--CURATED_BUCKET": CURATED_BUCKET,
        },
    )
    run_id = resp["JobRunId"]
    print(f"Started Glue job {job_name} run_id={run_id}")

    while True:
        run_resp = glue.get_job_run(JobName=job_name, RunId=run_id)
        run_state = run_resp["JobRun"]["JobRunState"]
        print(f"  Job {job_name}: {run_state}")

        if run_state == "SUCCEEDED":
            return run_id
        if run_state in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
            error_msg = run_resp["JobRun"].get("ErrorMessage", "no error detail")
            raise RuntimeError(f"Glue job {job_name} failed: {error_msg}")

        time.sleep(30)


def run_dq_checks(**context: dict) -> dict:
    """Execute data quality checks against curated Parquet via Athena."""
    dq_queries = ["null_card_ids", "invalid_currencies", "negative_settled_amounts"]

    if DEMO_MODE:
        time.sleep(random.uniform(2, 4))
        results = {q: 0 for q in dq_queries}
        print(f"[demo] All DQ checks passed: {results}")
        return results

    import boto3
    athena = boto3.client("athena", region_name=AWS_REGION)

    dq_sql = {
        "null_card_ids": """
            SELECT COUNT(*) AS null_count
            FROM "fintech_enterprise_catalog"."transactions"
            WHERE card_id IS NULL
        """,
        "invalid_currencies": """
            SELECT COUNT(*) AS bad_count
            FROM "fintech_enterprise_catalog"."transactions"
            WHERE transaction_currency NOT IN ('AED','USD','EGP','GBP','EUR','SAR','KWD')
        """,
        "negative_settled_amounts": """
            SELECT COUNT(*) AS bad_count
            FROM "fintech_enterprise_catalog"."transactions"
            WHERE status = 'SETTLED' AND CAST(billing_amount AS DOUBLE) <= 0
        """,
    }

    results: dict = {}
    failures: list[str] = []

    for check_name, query in dq_sql.items():
        resp = athena.start_query_execution(
            QueryString=query,
            WorkGroup="fintech-enterprise-workgroup",
            ResultConfiguration={
                "OutputLocation": f"s3://fintech-enterprise-athena-results/dq-checks/{context['run_id']}/"
            },
        )
        query_execution_id = resp["QueryExecutionId"]

        for _ in range(60):
            time.sleep(5)
            status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"DQ Athena query {check_name} failed: {status}")

        result = athena.get_query_results(QueryExecutionId=query_execution_id)
        count = int(result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])
        results[check_name] = count

        if count > 0:
            failures.append(f"{check_name}: {count} violations")

    if failures:
        raise ValueError(f"DQ checks FAILED:\n" + "\n".join(failures))

    print(f"All DQ checks passed: {results}")
    return results


def load_redshift(**context: dict) -> None:
    """Trigger Redshift data load via redshift-data API (serverless)."""
    if DEMO_MODE:
        time.sleep(random.uniform(3, 6))
        print("[demo] Redshift load completed (5 tables loaded)")
        return

    import boto3
    redshift_data = boto3.client("redshift-data", region_name=AWS_REGION)

    sql_path = "/opt/airflow/dags/copy_from_s3.sql"
    try:
        with open(sql_path) as f:
            sql_statements = f.read()
    except FileNotFoundError:
        print(f"WARNING: {sql_path} not found, skipping Redshift load")
        return

    account_id = boto3.client("sts").get_caller_identity()["Account"]
    sql_statements = sql_statements.replace("{ACCOUNT_ID}", account_id)
    sql_statements = sql_statements.replace("{REGION}", AWS_REGION)
    sql_statements = sql_statements.replace("{CURATED_BUCKET}", CURATED_BUCKET)

    resp = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql_statements,
    )
    statement_id = resp["Id"]
    print(f"Submitted Redshift load statement_id={statement_id}")

    for _ in range(120):
        time.sleep(15)
        status = redshift_data.describe_statement(Id=statement_id)
        state = status["Status"]
        print(f"  Redshift load status: {state}")
        if state == "FINISHED":
            print("Redshift load completed")
            return
        if state in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift load failed: {status.get('Error', 'unknown')}")

    raise TimeoutError("Redshift load timed out after 30 minutes")


def notify_success(**context: dict) -> None:
    """Publish success notification to SNS."""
    if DEMO_MODE:
        time.sleep(random.uniform(1, 2))
        print("[demo] Success notification sent")
        return

    if not SNS_TOPIC_ARN:
        print("No SNS_TOPIC_ARN configured, skipping notification")
        return

    import boto3
    sns = boto3.client("sns", region_name=AWS_REGION)
    run_id = context.get("run_id", "unknown")

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="[FINTECH_ENTERPRISE] Pipeline completed successfully",
        Message=(
            f"Pipeline: fintech_enterprise_aws_pipeline\n"
            f"Run ID: {run_id}\n"
            f"Completed at: {datetime.utcnow().isoformat()}Z\n"
            f"Status: SUCCESS"
        ),
    )
    print("Success notification sent")


# ── DAG definition ────────────────────────────────────────────────────────────────

with DAG(
    dag_id="fintech_enterprise_aws_pipeline",
    description="End-to-end Fintech Enterprise fintech data pipeline: S3 → Glue → Athena → Redshift",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    start_date=datetime(2026, 3, 23),
    catchup=False,
    max_active_runs=1,
    tags=["fintech_enterprise", "aws", "fintech", "etl"],
) as dag:

    t_check_s3 = PythonOperator(
        task_id="check_s3_raw",
        python_callable=check_s3_raw,
    )

    t_start_crawler = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=run_glue_crawler,
    )

    t_wait_crawler = PythonOperator(
        task_id="wait_crawler_complete",
        python_callable=wait_crawler_complete,
    )

    # Fan-out: run all 5 ETL jobs in parallel
    with TaskGroup("run_glue_etl") as tg_etl:
        etl_tasks = {}
        for source, job_name in GLUE_JOBS.items():
            etl_tasks[source] = PythonOperator(
                task_id=f"etl_{source}",
                python_callable=_run_glue_job,
                op_kwargs={"job_name": job_name},
            )

    t_dq = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_dq_checks,
    )

    t_load_redshift = PythonOperator(
        task_id="load_redshift",
        python_callable=load_redshift,
    )

    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────────
    t_check_s3 >> t_start_crawler >> t_wait_crawler >> tg_etl >> t_dq >> t_load_redshift >> t_notify
