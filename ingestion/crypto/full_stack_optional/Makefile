.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- lifecycle
.PHONY: up
up: ## Build and start the whole stack
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Airflow    http://localhost:8080   (admin / admin)"
	@echo "  Dashboard  http://localhost:8501"
	@echo "  MinIO      http://localhost:9001   (minioadmin / minioadmin)"
	@echo "  Spark UI   http://localhost:8081"

.PHONY: down
down: ## Stop the stack, keeping volumes
	$(COMPOSE) down

.PHONY: nuke
nuke: ## Stop the stack and delete all data
	$(COMPOSE) down -v --remove-orphans

.PHONY: logs
logs: ## Tail logs from every service
	$(COMPOSE) logs -f --tail=100

.PHONY: ps
ps: ## Show service status
	$(COMPOSE) ps

# ------------------------------------------------------------------ pipeline
.PHONY: trigger
trigger: ## Run the ingestion DAG once, now (transform follows via Dataset)
	$(COMPOSE) exec airflow-scheduler airflow dags unpause crypto_market_ingest
	$(COMPOSE) exec airflow-scheduler airflow dags unpause crypto_transform
	$(COMPOSE) exec airflow-scheduler airflow dags unpause crypto_assets_weekly
	$(COMPOSE) exec airflow-scheduler airflow dags trigger crypto_market_ingest

.PHONY: backfill
backfill: ## Backfill history: make backfill FROM=2024-01-01 TO=2026-09-01
	$(COMPOSE) exec airflow-scheduler airflow dags trigger crypto_backfill \
	  --conf '{"start_date":"$(FROM)","end_date":"$(TO)","symbols":"BTCUSDT,ETHUSDT,SOLUSDT","chunk_days":365}'

.PHONY: psql
psql: ## Open a psql shell on the warehouse
	$(COMPOSE) exec postgres psql -U warehouse -d market

# ----------------------------------------------------------------------- dbt
.PHONY: dbt-build
dbt-build: ## Run dbt build (models + tests)
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt build --profiles-dir . --target prod"

.PHONY: dbt-test
dbt-test: ## Run dbt tests only
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod"

.PHONY: dbt-snapshot
dbt-snapshot: ## Capture a reference-data snapshot
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt snapshot --profiles-dir . --target prod"

.PHONY: health
health: ## Show pipeline health SLIs from the warehouse
	$(COMPOSE) exec postgres psql -U warehouse -d market -c \
	  "SELECT dag_id, task_id, success_rate_pct, p95_duration_sec, failures_last_7d, health_status FROM analytics.mart_pipeline_health ORDER BY health_status, task_id;"

.PHONY: lineage
lineage: ## List everything downstream of a model: make lineage MODEL=fct_ohlcv_daily
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt ls --profiles-dir . --target prod --select $(MODEL)+"

.PHONY: hooks
hooks: ## Install the pre-commit hooks
	pip install pre-commit && pre-commit install && pre-commit install --hook-type pre-push

.PHONY: sqlfmt
sqlfmt: ## Lint the SQL models
	sqlfluff lint dbt/models --config .sqlfluff

.PHONY: dbt-docs
dbt-docs: ## Generate and serve the dbt documentation site on :8082
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . --target prod"
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs serve --profiles-dir . --port 8082"

# ------------------------------------------------------------------- quality
.PHONY: test
test: ## Run the Python unit tests
	PYTHONPATH=src pytest tests/ -v --cov=pipeline --cov-report=term-missing

.PHONY: lint
lint: ## Lint and type check
	ruff check src/ tests/ airflow/ dashboard/
	mypy src/pipeline --ignore-missing-imports

.PHONY: fmt
fmt: ## Auto-format the codebase
	ruff format src/ tests/ airflow/ dashboard/
	ruff check --fix src/ tests/ airflow/ dashboard/

.PHONY: install
install: ## Install dev dependencies locally
	pip install -r requirements-dev.txt
