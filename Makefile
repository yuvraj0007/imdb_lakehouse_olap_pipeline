# ═══════════════════════════════════════════════════════════════════════════
# IMDb Lakehouse to OLAP Pipeline - Makefile
# ═══════════════════════════════════════════════════════════════════════════
# Compatible with both Docker Compose and Podman Compose
# ═══════════════════════════════════════════════════════════════════════════

# Detect compose command (podman-compose or docker compose)
COMPOSE := $(shell command -v podman-compose 2>/dev/null || echo "docker compose")

# Project settings
PROJECT_NAME := imdb-pipeline
SPARK_MASTER := spark-master
CLICKHOUSE := clickhouse

.PHONY: help build up down restart logs download etl load analytics benchmark \
        clean status shell-spark shell-ch test test-unit test-integration \
        test-cov lint format monitoring-up monitoring-down

# ─────────────────────────────────────────────────────────────────────────────
# Default target
# ─────────────────────────────────────────────────────────────────────────────

help: ## Show this help message
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  IMDb Lakehouse → OLAP Pipeline"
	@echo "  Compose: $(COMPOSE)"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo ""
	@echo "  INFRASTRUCTURE"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '(build|up|down|restart|status|logs)' | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  PIPELINE"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '(download|etl|load|analytics|pipeline|benchmark)' | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  TESTING & QUALITY"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '(test|lint|format|cov)' | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  MONITORING"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '(monitoring|grafana|prometheus)' | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  DEVELOPMENT"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '(shell|pyspark|query|setup|clean)' | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Infrastructure
# ─────────────────────────────────────────────────────────────────────────────

build: ## Build all Docker images
	$(COMPOSE) build

up: ## Start all services (Spark + ClickHouse + Monitoring)
	$(COMPOSE) up -d
	@echo "Waiting for services to be healthy..."
	@sleep 15
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  Services started!"
	@echo "  • Spark Master UI:   http://localhost:8080"
	@echo "  • ClickHouse HTTP:   http://localhost:8123"
	@echo "  • Grafana:           http://localhost:3000  (admin/admin)"
	@echo "  • Prometheus:        http://localhost:9090"
	@echo "═══════════════════════════════════════════════════════════════"

down: ## Stop and remove all services
	$(COMPOSE) down

restart: down up ## Restart all services

status: ## Show service status
	$(COMPOSE) ps

logs: ## Show all service logs (follow mode)
	$(COMPOSE) logs -f

logs-spark: ## Show Spark master logs
	$(COMPOSE) logs -f $(SPARK_MASTER)

logs-ch: ## Show ClickHouse logs
	$(COMPOSE) logs -f $(CLICKHOUSE)

logs-grafana: ## Show Grafana logs
	$(COMPOSE) logs -f grafana

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Steps
# ─────────────────────────────────────────────────────────────────────────────

download: ## Download IMDb dataset from Kaggle
	@echo "Downloading IMDb dataset..."
	python3 src/download.py

etl: ## Run PySpark ETL pipeline (transform → Parquet)
	@echo "Running ETL pipeline..."
	$(COMPOSE) exec $(SPARK_MASTER) spark-submit \
		--master spark://spark-master:7077 \
		--driver-memory 2g \
		--executor-memory 2g \
		/opt/spark/src/etl/pipeline.py

load: ## Load Parquet data into ClickHouse
	@echo "Loading data into ClickHouse..."
	$(COMPOSE) exec $(SPARK_MASTER) python3 \
		/opt/spark/src/olap/pipeline.py

analytics: ## Run analytics benchmark (Spark vs ClickHouse)
	@echo "Running analytics benchmark..."
	$(COMPOSE) exec $(SPARK_MASTER) python3 \
		/opt/spark/src/analytics/benchmark.py

# ─────────────────────────────────────────────────────────────────────────────
# Full Pipeline (end-to-end)
# ─────────────────────────────────────────────────────────────────────────────

pipeline: up etl load analytics ## Run full pipeline (start → ETL → Load → Analytics)
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  Pipeline complete! ✓"
	@echo "═══════════════════════════════════════════════════════════════"

benchmark: analytics ## Alias for analytics benchmark

# ─────────────────────────────────────────────────────────────────────────────
# Testing & Code Quality
# ─────────────────────────────────────────────────────────────────────────────

test: test-unit test-integration ## Run all tests

test-unit: ## Run unit tests
	@echo "Running unit tests..."
	pytest tests/test_etl_job.py -v --tb=short

test-integration: ## Run integration tests
	@echo "Running integration tests..."
	pytest tests/test_integration.py -v --timeout=120 --tb=short

test-analytics: ## Run analytics tests
	@echo "Running analytics tests..."
	pytest tests/test_analytics.py -v --tb=short

test-cov: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	pytest tests/ -v \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-report=xml:coverage.xml
	@echo ""
	@echo "Coverage report: htmlcov/index.html"

lint: ## Run code linters (flake8, black --check, isort --check)
	@echo "Running linters..."
	flake8 src/ tests/ --max-line-length=120 --extend-ignore=E203,W503
	black --check --line-length=120 src/ tests/
	isort --check --profile=black src/ tests/
	@echo "✓ All lint checks passed"

format: ## Auto-format code (black + isort)
	@echo "Formatting code..."
	black --line-length=120 src/ tests/
	isort --profile=black src/ tests/
	@echo "✓ Code formatted"

# ─────────────────────────────────────────────────────────────────────────────
# Monitoring
# ─────────────────────────────────────────────────────────────────────────────

monitoring-up: ## Start only monitoring stack (Prometheus + Grafana)
	$(COMPOSE) up -d prometheus grafana node-exporter
	@echo ""
	@echo "Monitoring started:"
	@echo "  • Grafana:    http://localhost:3000  (admin/admin)"
	@echo "  • Prometheus: http://localhost:9090"

monitoring-down: ## Stop monitoring stack
	$(COMPOSE) stop prometheus grafana node-exporter

grafana-open: ## Open Grafana in browser
	@open http://localhost:3000 2>/dev/null || xdg-open http://localhost:3000 2>/dev/null || echo "Open http://localhost:3000"

prometheus-open: ## Open Prometheus in browser
	@open http://localhost:9090 2>/dev/null || xdg-open http://localhost:9090 2>/dev/null || echo "Open http://localhost:9090"

# ─────────────────────────────────────────────────────────────────────────────
# Development & Debugging
# ─────────────────────────────────────────────────────────────────────────────

shell-spark: ## Open bash shell in Spark master container
	$(COMPOSE) exec $(SPARK_MASTER) bash

shell-ch: ## Open ClickHouse client shell
	$(COMPOSE) exec $(CLICKHOUSE) clickhouse-client --password $${CLICKHOUSE_PASSWORD:-clickhouse}

query: ## Run a ClickHouse query (usage: make query SQL="SELECT count() FROM imdb.imdb_titles_enriched")
	$(COMPOSE) exec $(CLICKHOUSE) clickhouse-client \
		--password $${CLICKHOUSE_PASSWORD:-clickhouse} \
		--query "$(SQL)"

pyspark: ## Open PySpark interactive shell
	$(COMPOSE) exec $(SPARK_MASTER) pyspark --master spark://spark-master:7077

# ─────────────────────────────────────────────────────────────────────────────
# Setup & Cleanup
# ─────────────────────────────────────────────────────────────────────────────

setup: ## Install Python dependencies locally
	pip install -r requirements.txt
	@echo "✓ Dependencies installed"

clean: ## Remove generated data (lake + staging)
	rm -rf data/lake/*
	rm -rf data/staging/*
	rm -rf htmlcov/ coverage.xml .coverage
	rm -rf tests/__pycache__/ src/__pycache__/
	@echo "✓ Cleaned generated data and cache"

clean-all: clean down ## Remove all data and stop services
	rm -rf data/raw/*
	$(COMPOSE) down -v
	@echo "✓ Cleaned all data and removed volumes"

# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

validate: validate-compose check-health ## Validate all configurations and services

validate-compose: ## Validate docker-compose.yml syntax
	@echo "Validating compose file..."
	$(COMPOSE) config > /dev/null
	@echo "✓ docker-compose.yml is valid"

check-health: ## Check health of all services
	@echo "Service health:"
	@echo "  Spark Master: $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080 2>/dev/null || echo 'DOWN')"
	@echo "  ClickHouse:   $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8123/ping 2>/dev/null || echo 'DOWN')"
	@echo "  Prometheus:   $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:9090/-/healthy 2>/dev/null || echo 'DOWN')"
	@echo "  Grafana:      $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:3000/api/health 2>/dev/null || echo 'DOWN')"
