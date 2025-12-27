.PHONY: help init install sync lock hooks commit \
        lint format test test-cov ci \
        dagster dbt-run dbt-test dbt-build dbt-seed dbt-compile dbt-docs dbt-docs-serve dbt-deps dbt-clean \
        build run stop logs shell compose-up compose-down compose-logs \
        db-shell clean

# Default target
help:
	@echo ""
	@echo "                    /\\"
	@echo "                   /  \\"
	@echo "                  / py \\"
	@echo "                 /______\\"
	@echo "                /\\      /\\"
	@echo "               /  \\    /  \\"
	@echo "              / dbt\\  / dag\\"
	@echo "             /______\\/______\\"
	@echo ""
	@echo "   Prism: Risk Adjustment Analytics Platform"
	@echo ""
	@echo "Getting Started:"
	@echo "  make init          Full project setup (install + hooks)"
	@echo "  make install       Install all dependencies with uv"
	@echo "  make sync          Sync dependencies from lockfile"
	@echo "  make lock          Update lockfile"
	@echo "  make hooks         Install pre-commit hooks"
	@echo ""
	@echo "Development:"
	@echo "  make commit        Commit using commitizen conventions"
	@echo "  make ci            Run all CI checks locally"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint          Run ruff linter"
	@echo "  make format        Format code with ruff"
	@echo "  make test          Run tests"
	@echo "  make test-cov      Run tests with coverage"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt-run       Run dbt models"
	@echo "  make dbt-test      Run dbt tests"
	@echo "  make dbt-build     Run dbt build (run + test)"
	@echo "  make dbt-seed      Load dbt seed data"
	@echo "  make dbt-compile   Compile dbt models (no execution)"
	@echo "  make dbt-docs      Generate dbt documentation"
	@echo "  make dbt-docs-serve Serve dbt docs locally"
	@echo "  make dbt-deps      Install dbt packages"
	@echo "  make dbt-clean     Clean dbt artifacts"
	@echo ""
	@echo "Services:"
	@echo "  make dagster       Start Dagster dev server"
	@echo "  make db-shell      Open DuckDB interactive shell"
	@echo ""
	@echo "Container:"
	@echo "  make build         Build container image"
	@echo "  make run           Run container"
	@echo "  make stop          Stop container"
	@echo "  make logs          View container logs"
	@echo "  make shell         Shell into container"
	@echo ""
	@echo "Compose:"
	@echo "  make compose-up    Start all services"
	@echo "  make compose-down  Stop all services"
	@echo "  make compose-logs  View logs from all services"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         Remove build artifacts"

# =============================================================================
# Getting Started
# =============================================================================

init: install hooks

install:
	uv sync --all-extras

sync:
	uv sync --all-extras --frozen

lock:
	uv lock

hooks:
	uv run pre-commit install

# =============================================================================
# Development
# =============================================================================

commit:
	uv run cz commit

ci: lint test dbt-compile

# =============================================================================
# Code Quality
# =============================================================================

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check --fix .

test:
	uv run pytest

test-cov:
	uv run pytest --cov=calculators --cov=platform --cov-report=term-missing --cov-report=html

# =============================================================================
# Services
# =============================================================================

dagster:
	uv run dagster dev -m platform.definitions

db-shell:
	uv run duckdb data/prism.duckdb

# =============================================================================
# dbt
# =============================================================================

dbt-run:
	cd dbt && uv run dbt run

dbt-test:
	cd dbt && uv run dbt test

dbt-build:
	cd dbt && uv run dbt build

dbt-seed:
	cd dbt && uv run dbt seed

dbt-compile:
	cd dbt && uv run dbt compile

dbt-docs:
	cd dbt && uv run dbt docs generate

dbt-docs-serve:
	cd dbt && uv run dbt docs serve

dbt-deps:
	cd dbt && uv run dbt deps

dbt-clean:
	cd dbt && uv run dbt clean

# =============================================================================
# Container (Podman)
# =============================================================================

CONTAINER_NAME := prism
IMAGE_NAME := prism:latest

build:
	podman build -t $(IMAGE_NAME) .

run:
	podman run -d --name $(CONTAINER_NAME) \
		-p 3000:3000 \
		-v $(PWD)/data:/app/data:Z \
		$(IMAGE_NAME)

stop:
	podman stop $(CONTAINER_NAME) && podman rm $(CONTAINER_NAME)

logs:
	podman logs -f $(CONTAINER_NAME)

shell:
	podman exec -it $(CONTAINER_NAME) /bin/bash

# =============================================================================
# Compose (Podman)
# =============================================================================

compose-up:
	podman-compose up -d

compose-down:
	podman-compose down

compose-logs:
	podman-compose logs -f

# =============================================================================
# Cleanup
# =============================================================================

clean:
	rm -rf .venv
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf dist
	rm -rf *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
