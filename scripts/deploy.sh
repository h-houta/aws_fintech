#!/usr/bin/env bash
# =============================================================================
# Script: deploy.sh
# Purpose: Full deployment — Terraform apply, upload data, verify pipeline
# Usage: bash scripts/deploy.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo "  Fintech Enterprise AWS Pipeline — Full Deployment"
echo "=============================================="
echo ""

# ── Step 1: Validate prerequisites ────────────────────────────────────────────────
echo "[1/5] Checking prerequisites..."

command -v terraform >/dev/null 2>&1 || { echo "ERROR: terraform not found"; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI not found"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found"; exit 1; }

aws sts get-caller-identity --query "Account" --output text >/dev/null 2>&1 \
    || { echo "ERROR: AWS credentials not configured (run: aws configure)"; exit 1; }

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
echo "  AWS Account: ${AWS_ACCOUNT_ID}"
echo "  Region: ${AWS_REGION:-us-east-1}"
echo "  ✓ Prerequisites OK"
echo ""

# ── Step 2: Anonymize PII ─────────────────────────────────────────────────────────
echo "[2/5] Anonymizing PII in users.csv..."
if [[ -f "$PROJECT_ROOT/data/raw/users.csv" ]]; then
    python3 "$PROJECT_ROOT/scripts/anonymize_data.py"
    echo "  ✓ users_anonymized.csv created"
else
    echo "  SKIP: data/raw/users.csv not found"
fi
echo ""

# ── Step 3: Terraform ─────────────────────────────────────────────────────────────
echo "[3/5] Deploying infrastructure with Terraform..."
cd "$PROJECT_ROOT/terraform"

if [[ ! -d ".terraform" ]]; then
    echo "  Initializing Terraform..."
    terraform init
fi

terraform plan -out=tfplan
echo ""
read -rp "  Apply Terraform plan? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "  Aborting."
    exit 0
fi

terraform apply tfplan
echo "  ✓ Terraform apply complete"
echo ""

# ── Step 4: Upload raw data ───────────────────────────────────────────────────────
echo "[4/5] Uploading raw CSVs to S3..."
cd "$PROJECT_ROOT"

# Get the actual bucket name from Terraform output (includes account ID suffix)
RAW_BUCKET=$(terraform -chdir=terraform output -raw raw_bucket_name 2>/dev/null || echo "fintech-enterprise-data-lake-raw-${AWS_ACCOUNT_ID}")
export RAW_BUCKET

bash scripts/upload_to_s3.sh
echo "  ✓ Data upload complete"
echo ""

# ── Step 5: Verify ───────────────────────────────────────────────────────────────
echo "[5/5] Verifying deployment..."
echo ""

CRAWLER_NAME=$(terraform -chdir=terraform output -raw glue_crawler_name 2>/dev/null || echo "fintech-enterprise-raw-crawler")
echo "  Glue Crawler: ${CRAWLER_NAME}"
aws glue get-crawler --name "${CRAWLER_NAME}" --query "Crawler.State" --output text \
    && echo "  ✓ Crawler accessible"

echo ""
echo "=============================================="
echo "  Deployment complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Trigger pipeline: make airflow-up, then manually trigger fintech_enterprise_aws_pipeline DAG"
echo "  2. Monitor Glue: AWS Console → Glue → Jobs"
echo "  3. Query data: AWS Console → Athena → Workgroup: fintech-enterprise-workgroup"
echo "  4. View dashboard: make dashboard"
echo "  5. Teardown: make teardown"
