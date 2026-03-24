#!/usr/bin/env bash
# =============================================================================
# Script: teardown.sh
# Purpose: Destroy ALL AWS resources for this project (irreversible)
#          Empties S3 buckets before Terraform destroy (S3 bucket deletion
#          requires the bucket to be empty)
# WARNING: This will permanently delete all data and infrastructure.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo "  ⚠️  FINTECH_ENTERPRISE AWS PIPELINE — TEARDOWN"
echo "  This will destroy ALL AWS resources."
echo "=============================================="
echo ""
read -rp "Type 'destroy' to confirm: " confirm
if [[ "$confirm" != "destroy" ]]; then
    echo "Cancelled."
    exit 0
fi

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)

# ── Step 1: Empty S3 buckets (required before Terraform destroy) ──────────────────
echo ""
echo "[1/3] Emptying S3 buckets..."

BUCKETS=(
    "fintech-enterprise-data-lake-raw-${ACCOUNT_ID}"
    "fintech-enterprise-data-lake-curated-${ACCOUNT_ID}"
    "fintech-enterprise-athena-results-${ACCOUNT_ID}"
    "fintech-enterprise-glue-scripts-${ACCOUNT_ID}"
)

for bucket in "${BUCKETS[@]}"; do
    if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        echo "  Emptying: s3://${bucket}/"
        aws s3 rm "s3://${bucket}/" --recursive --region "$REGION" 2>/dev/null || true
        # Remove versioned objects
        aws s3api delete-objects \
            --bucket "$bucket" \
            --delete "$(aws s3api list-object-versions \
                --bucket "$bucket" \
                --query '{Objects: Versions[].{Key: Key, VersionId: VersionId}}' \
                --output json)" \
            2>/dev/null || true
        echo "  ✓ Emptied: ${bucket}"
    else
        echo "  SKIP (not found): ${bucket}"
    fi
done

# ── Step 2: Terraform destroy ─────────────────────────────────────────────────────
echo ""
echo "[2/3] Running terraform destroy..."
cd "$PROJECT_ROOT/terraform"
terraform destroy -auto-approve
echo "  ✓ Terraform destroy complete"

# ── Step 3: Clean local artifacts ─────────────────────────────────────────────────
echo ""
echo "[3/3] Cleaning local artifacts..."
rm -f "$PROJECT_ROOT/terraform/tfplan"
rm -f "$PROJECT_ROOT/lambda/s3_trigger.zip"
echo "  ✓ Local artifacts cleaned"

echo ""
echo "=============================================="
echo "  Teardown complete. AWS costs stopped."
echo "=============================================="
