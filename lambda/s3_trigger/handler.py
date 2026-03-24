"""
Lambda Function: fintech-enterprise-s3-glue-trigger
=======================================
Triggered by S3 PUT events on the raw bucket (*.csv files).

Flow:
  1. Parse the S3 event to determine which source landed (orgs/users/cards/etc.)
  2. Start the Glue Crawler to update the Data Catalog
  3. Wait for crawler to complete (poll with exponential backoff)
  4. Start all relevant Glue ETL jobs in parallel
  5. On any failure → publish SNS alert

Environment variables (set by Terraform):
  GLUE_CRAWLER_NAME, GLUE_JOB_TRANSACTIONS, GLUE_JOB_CLEARING,
  GLUE_JOB_CARDS, GLUE_JOB_USERS, GLUE_JOB_ORGS,
  SNS_ALERT_TOPIC_ARN, RAW_BUCKET, CURATED_BUCKET
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Env vars ──────────────────────────────────────────────────────────────────────

CRAWLER_NAME = os.environ["GLUE_CRAWLER_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_ALERT_TOPIC_ARN"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
CURATED_BUCKET = os.environ["CURATED_BUCKET"]

# Map S3 prefix → Glue job env var name
PREFIX_TO_JOB_ENV = {
    "transactions/": "GLUE_JOB_TRANSACTIONS",
    "clearing/": "GLUE_JOB_CLEARING",
    "cards/": "GLUE_JOB_CARDS",
    "users/": "GLUE_JOB_USERS",
    "orgs/": "GLUE_JOB_ORGS",
}

glue_client = boto3.client("glue")
sns_client = boto3.client("sns")


# ── SNS helpers ───────────────────────────────────────────────────────────────────

def publish_alert(subject: str, message: str) -> None:
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
        )
    except ClientError as e:
        logger.error("Failed to publish SNS alert: %s", e)


# ── Glue Crawler ──────────────────────────────────────────────────────────────────

def start_crawler() -> None:
    """Start crawler, ignore if already running."""
    try:
        glue_client.start_crawler(Name=CRAWLER_NAME)
        logger.info("Started crawler: %s", CRAWLER_NAME)
    except glue_client.exceptions.CrawlerRunningException:
        logger.info("Crawler already running, continuing...")


def wait_for_crawler(max_wait_seconds: int = 600, poll_interval: int = 15) -> bool:
    """Poll crawler state until READY or failure. Returns True on success."""
    elapsed = 0
    while elapsed < max_wait_seconds:
        resp = glue_client.get_crawler(Name=CRAWLER_NAME)
        state = resp["Crawler"]["State"]
        logger.info("Crawler state: %s (elapsed: %ds)", state, elapsed)

        if state == "READY":
            last_status = resp["Crawler"].get("LastCrawl", {})
            if last_status.get("Status") == "FAILED":
                logger.error("Crawler finished with FAILED status: %s", last_status)
                return False
            return True

        time.sleep(poll_interval)
        elapsed += poll_interval

    logger.error("Crawler timed out after %ds", max_wait_seconds)
    return False


# ── Glue ETL Jobs ─────────────────────────────────────────────────────────────────

def get_jobs_for_prefixes(prefixes: List[str]) -> List[str]:
    """Return Glue job names for the given S3 prefixes."""
    job_names = []
    for prefix in prefixes:
        for key, env_var in PREFIX_TO_JOB_ENV.items():
            if prefix.startswith(key.rstrip("/")):
                job_name = os.environ.get(env_var)
                if job_name and job_name not in job_names:
                    job_names.append(job_name)
    return job_names


def start_glue_jobs(job_names: List[str]) -> Dict[str, str]:
    """Start Glue ETL jobs in parallel. Returns {job_name: run_id}."""
    run_ids: Dict[str, str] = {}
    job_args = {
        "--RAW_BUCKET": RAW_BUCKET,
        "--CURATED_BUCKET": CURATED_BUCKET,
    }

    for job_name in job_names:
        try:
            resp = glue_client.start_job_run(JobName=job_name, Arguments=job_args)
            run_id = resp["JobRunId"]
            run_ids[job_name] = run_id
            logger.info("Started Glue job %s → run_id=%s", job_name, run_id)
        except ClientError as e:
            logger.error("Failed to start Glue job %s: %s", job_name, e)
            publish_alert(
                subject=f"[FINTECH_ENTERPRISE] Glue job start FAILED: {job_name}",
                message=f"Job: {job_name}\nError: {str(e)}",
            )
    return run_ids


# ── Main handler ──────────────────────────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Event: %s", json.dumps(event))

    # Extract S3 keys from event
    s3_keys: List[str] = []
    for record in event.get("Records", []):
        key = record.get("s3", {}).get("object", {}).get("key", "")
        if key:
            s3_keys.append(key)
            logger.info("S3 object: s3://%s/%s", RAW_BUCKET, key)

    if not s3_keys:
        logger.warning("No S3 keys found in event, skipping")
        return {"statusCode": 200, "body": "No S3 keys to process"}

    # Determine which sources landed
    touched_prefixes = list({key.split("/")[0] for key in s3_keys if "/" in key})
    logger.info("Touched source prefixes: %s", touched_prefixes)

    # Step 1: Start crawler
    start_crawler()

    # Step 2: Wait for crawler
    crawler_ok = wait_for_crawler()
    if not crawler_ok:
        publish_alert(
            subject="[FINTECH_ENTERPRISE] Glue Crawler FAILED",
            message=f"Crawler {CRAWLER_NAME} failed or timed out.\nS3 keys: {s3_keys}",
        )
        return {"statusCode": 500, "body": "Crawler failed"}

    # Step 3: Start ETL jobs for touched sources
    job_names = get_jobs_for_prefixes(touched_prefixes)
    if not job_names:
        # No specific jobs matched — run all
        job_names = [os.environ[v] for v in PREFIX_TO_JOB_ENV.values() if os.environ.get(v)]

    run_ids = start_glue_jobs(job_names)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Pipeline triggered",
            "crawler": CRAWLER_NAME,
            "jobs_started": run_ids,
        }),
    }
