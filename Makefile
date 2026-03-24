.PHONY: help lint test format deploy teardown airflow-up airflow-down upload-raw clean

PYTHON := python3
TERRAFORM := terraform
PROJECT_ROOT := $(shell pwd)
AWS_REGION := us-east-1
S3_RAW_BUCKET := fintech-enterprise-data-lake-raw
S3_CURATED_BUCKET := fintech-enterprise-data-lake-curated

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Code Quality ────────────────────────────────────────────────────────────────

lint:  ## Run all linters (black check, isort check, flake8, mypy)
	black --check --diff glue/ lambda/ streamlit/ tests/
	isort --check-only --diff glue/ lambda/ streamlit/ tests/
	flake8 glue/ lambda/ streamlit/ tests/
	mypy glue/ lambda/

format:  ## Auto-format code with black + isort
	black glue/ lambda/ streamlit/ tests/
	isort glue/ lambda/ streamlit/ tests/

# ── Testing ─────────────────────────────────────────────────────────────────────

test:  ## Run unit tests with pytest
	$(PYTHON) -m pytest tests/ -v

test-coverage:  ## Run tests with HTML coverage report
	$(PYTHON) -m pytest tests/ -v --cov=glue --cov=lambda --cov-report=html
	@echo "Coverage report: htmlcov/index.html"

# ── Data Preparation ────────────────────────────────────────────────────────────

anonymize:  ## Hash PII in users.csv before any uploads
	$(PYTHON) scripts/anonymize_data.py

# ── AWS Deployment ───────────────────────────────────────────────────────────────

init:  ## Initialize Terraform
	cd terraform && $(TERRAFORM) init

plan:  ## Terraform plan (dry run)
	cd terraform && $(TERRAFORM) plan -out=tfplan

deploy:  ## Full deploy: Terraform apply + upload raw data + trigger pipeline
	cd terraform && $(TERRAFORM) apply -auto-approve
	bash scripts/upload_to_s3.sh
	@echo "✓ Infrastructure deployed and data uploaded"

upload-raw:  ## Upload raw CSVs to S3 raw bucket
	bash scripts/upload_to_s3.sh

teardown:  ## Destroy ALL AWS resources (irreversible!)
	bash scripts/teardown.sh

# ── Airflow (Local Docker) ───────────────────────────────────────────────────────

airflow-up:  ## Start Airflow stack in Docker
	cd airflow && docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"

airflow-down:  ## Stop Airflow stack
	cd airflow && docker compose down

airflow-logs:  ## Tail Airflow scheduler logs
	cd airflow && docker compose logs -f scheduler

# ── Streamlit Dashboard ──────────────────────────────────────────────────────────

dashboard:  ## Launch local Streamlit BI dashboard
	cd streamlit && pip install -r requirements.txt -q && streamlit run app.py

# ── Utilities ────────────────────────────────────────────────────────────────────

clean:  ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/ *.egg-info/
	@echo "✓ Cleaned"

install-dev:  ## Install all dev dependencies
	pip install -e ".[dev]"
