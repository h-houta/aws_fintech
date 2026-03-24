#!/usr/bin/env bash
# =============================================================================
# Script: upload_to_s3.sh
# Purpose: Upload anonymized raw CSV files to S3 raw landing bucket
# Prerequisites:
#   - AWS CLI configured (aws configure or IAM role)
#   - Anonymized CSVs exist in data/raw/ (run: make anonymize first for users.csv)
#   - RAW_BUCKET env var set (or edit BUCKET below)
# Usage: bash scripts/upload_to_s3.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data/raw"

# Read from env or use placeholder (Terraform sets actual name with account ID suffix)
BUCKET="${RAW_BUCKET:-fintech-enterprise-data-lake-raw}"
REGION="${AWS_REGION:-us-east-1}"

echo "Uploading raw CSV files to s3://${BUCKET}/"
echo "Region: ${REGION}"
echo ""

# Mapping: local filename → S3 prefix
declare -A FILE_TO_PREFIX=(
    ["orgs.csv"]="orgs"
    ["users.csv"]="users"
    ["cards.csv"]="cards"
    ["transactions.csv"]="transactions"
    ["clearing.csv"]="clearing"
)

UPLOADED=0
SKIPPED=0

for filename in "${!FILE_TO_PREFIX[@]}"; do
    local_path="$DATA_DIR/$filename"
    s3_prefix="${FILE_TO_PREFIX[$filename]}"

    if [[ -f "$local_path" ]]; then
        s3_key="${s3_prefix}/${filename}"
        echo "  Uploading: $filename → s3://${BUCKET}/${s3_key}"
        aws s3 cp "$local_path" "s3://${BUCKET}/${s3_key}" \
            --region "$REGION" \
            --sse AES256 \
            --metadata "project=fintech-enterprise-aws-pipeline,uploaded-by=upload_to_s3.sh"
        UPLOADED=$((UPLOADED + 1))
    else
        echo "  SKIPPING: $local_path not found"
        SKIPPED=$((SKIPPED + 1))
    fi
done

echo ""
echo "Done: ${UPLOADED} file(s) uploaded, ${SKIPPED} skipped."

if [[ $SKIPPED -gt 0 ]]; then
    echo "WARNING: Some files were not found. Ensure data/raw/ contains your CSVs."
    echo "  For users.csv: run 'make anonymize' first to generate users_anonymized.csv"
fi
