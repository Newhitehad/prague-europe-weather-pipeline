include .env
export

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
RUFF := $(VENV)/bin/ruff
BLACK := $(VENV)/bin/black
PYTEST := $(VENV)/bin/pytest
BOOTSTRAP_STAMP := $(VENV)/.bootstrap-installed
DEPS_STAMP := $(VENV)/.deps-installed
SMOKE_RAW_DATA_DIR := tests/fixtures/raw
UI_HOST ?= 127.0.0.1
UI_PORT ?= 8000

.PHONY: help venv install install-cli lint format test verify refresh smoke up down wait init ingest raw stage mart dq check ui db-up db-down db-logs db-wait db-setup run-ingestion db-create-raw-tables load-raw db-create-staging-tables load-staging db-create-mart-tables load-mart quality-checks

help:
	@echo "make venv                     - Create or repair the local virtual environment"
	@echo "make install                  - Install project dependencies into the local virtual environment"
	@echo "make install-cli              - Symlink prague-weather into ~/.local/bin for global terminal use"
	@echo "make lint                     - Run ruff lint checks"
	@echo "make format                   - Run black formatting"
	@echo "make test                     - Run pytest"
	@echo "make verify                   - Run the local code-quality checks (lint + tests)"
	@echo "make refresh                  - Run the full local pipeline from ingestion to quality checks"
	@echo "make smoke                    - Run a deterministic DB smoke test with fixture raw data"
	@echo "make ui                       - Start the FastAPI read-only analytics dashboard (override UI_PORT/UI_HOST if needed)"
	@echo "make init                     - Alias for make refresh"
	@echo "make check                    - Alias for make verify"
	@echo "make up / down / wait         - Short aliases for DB lifecycle commands"
	@echo "make ingest / raw / stage     - Short aliases for pipeline loading steps"
	@echo "make mart / dq                - Short aliases for mart load and data quality"
	@echo "make db-up                    - Start Postgres via Docker Compose"
	@echo "make db-down                  - Stop Postgres containers"
	@echo "make db-logs                  - Tail Postgres logs"
	@echo "make db-wait                  - Wait until Postgres is ready to accept connections"
	@echo "make db-setup                 - Create DB schemas (raw, staging, mart)"
	@echo "make run-ingestion            - Run weather ingestion and write raw JSON files"
	@echo "make db-create-raw-tables     - Create raw layer tables"
	@echo "make load-raw                 - Load raw JSON files into PostgreSQL raw schema"
	@echo "make db-create-staging-tables - Create staging layer tables"
	@echo "make load-staging             - Transform and load raw data into staging tables"
	@echo "make db-create-mart-tables    - Create mart layer tables"
	@echo "make load-mart                - Build mart tables from staging data"
	@echo "make quality-checks           - Run mart-layer data quality checks"

$(VENV)/pyvenv.cfg:
	python3 -m venv $(VENV)

venv: $(VENV)/pyvenv.cfg
	@$(PYTHON) -m ensurepip --upgrade >/dev/null

$(BOOTSTRAP_STAMP): $(VENV)/pyvenv.cfg
	@$(PYTHON) -m ensurepip --upgrade >/dev/null
	$(PIP) install "setuptools>=68" wheel
	@touch $(BOOTSTRAP_STAMP)

$(DEPS_STAMP): pyproject.toml $(BOOTSTRAP_STAMP)
	$(PIP) install --no-build-isolation -e '.[dev]'
	@touch $(DEPS_STAMP)

install: $(DEPS_STAMP)

install-cli: $(DEPS_STAMP)
	mkdir -p "$(HOME)/.local/bin"
	ln -sf "$$(pwd)/.venv/bin/prague-weather" "$(HOME)/.local/bin/prague-weather"
	@echo "Linked prague-weather to $(HOME)/.local/bin/prague-weather"

lint: $(DEPS_STAMP)
	$(RUFF) check src tests

format: $(DEPS_STAMP)
	$(BLACK) src tests

test: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTEST) -q

verify: lint test

refresh: db-up db-wait db-setup db-create-raw-tables db-create-staging-tables db-create-mart-tables run-ingestion load-raw load-staging load-mart quality-checks

db-up:
	docker compose up -d

db-down:
	docker compose down

db-logs:
	docker compose logs -f postgres

db-wait: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.wait_for_db --timeout 60 --interval 1

db-setup: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.sql_runner sql/001_create_schemas.sql

db-create-raw-tables: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.sql_runner sql/002_create_raw_tables.sql

run-ingestion: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.main

load-raw: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.load.load_raw

db-create-staging-tables: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.sql_runner sql/003_create_staging_tables.sql

load-staging: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.sql_runner sql/004_load_staging_from_raw.sql

db-create-mart-tables: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.sql_runner sql/005_create_mart_tables.sql

load-mart: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.sql_runner sql/006_load_mart_from_staging.sql

quality-checks: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m pipeline.quality.checks

smoke: db-wait db-setup db-create-raw-tables db-create-staging-tables db-create-mart-tables
	RAW_DATA_DIR=$(SMOKE_RAW_DATA_DIR) PYTHONPATH=src $(PYTHON) -m pipeline.load.load_raw
	PYTHONPATH=src $(PYTHON) -m pipeline.sql_runner sql/004_load_staging_from_raw.sql sql/006_load_mart_from_staging.sql
	PYTHONPATH=src $(PYTHON) -m pipeline.quality.checks

ui: $(DEPS_STAMP)
	PYTHONPATH=src $(PYTHON) -m uvicorn pipeline.dashboard.app:app --host $(UI_HOST) --port $(UI_PORT)

up: db-up

down: db-down

wait: db-wait

init: refresh

ingest: run-ingestion

raw: load-raw

stage: load-staging

mart: load-mart

dq: quality-checks

check: verify
