# Prism Requirements

Complete installation requirements derived from ADR.md and implementation_plan.md.  
Organized by installation context: OS-level, Python (uv/pip), and Container.

---

## Quick Reference

| Layer | Tool | Install Method |
|-------|------|----------------|
| **OS** | Git, Podman, Make | System package manager |
| **Python** | All Python packages | `uv sync` from pyproject.toml |
| **Container** | Python runtime, uv | Dockerfile |

---

## 1. OS-Level Installations

Install these on your development machine using your system package manager.

### Required

| Tool | Purpose | Install (Ubuntu/Debian) | Install (macOS) |
|------|---------|-------------------------|-----------------|
| **Git** | Version control | `sudo apt install git` | `brew install git` |
| **Podman** | Container runtime (Docker alternative) | `sudo apt install podman` | `brew install podman` |
| **Make** | Build automation | `sudo apt install make` | Pre-installed / `xcode-select --install` |
| **Python 3.11+** | Runtime | `sudo apt install python3.11` | `brew install python@3.11` |

### Verification

```bash
git --version          # >= 2.30
podman --version       # >= 4.0
make --version         # GNU Make >= 4.0
python3 --version      # >= 3.11
```

---

## 2. Python Packages (uv/pip)

All Python dependencies are managed via `uv` and declared in `pyproject.toml`.

### Installing uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Project Dependencies

Run `uv sync` to install all dependencies from `pyproject.toml`.

#### Core Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| **dagster** | latest | Orchestration framework |
| **dagster-dbt** | latest | dbt integration for Dagster |
| **dagster-webserver** | latest | Dagster UI |
| **dbt-duckdb** | latest | dbt adapter for DuckDB |
| **duckdb** | latest | Embedded analytics database |
| **hccpy** | latest | HCC risk scoring calculator |
| **pandas** | latest | DataFrame operations |
| **polars** | latest | High-performance DataFrames |
| **pydantic** | >=2.0 | Data validation & settings |
| **pyyaml** | latest | YAML config parsing |
| **rich** | latest | Beautiful terminal output |
| **tabulate** | latest | Table formatting |
| **typer** | latest | CLI framework |
| **httpx** | latest | Async HTTP client |

#### Dev Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| **pytest** | latest | Testing framework |
| **pytest-cov** | latest | Coverage reporting |
| **ruff** | latest | Linting & formatting |
| **commitizen** | latest | Commit conventions |
| **pre-commit** | latest | Git hooks |

### pyproject.toml Reference

```toml
[project]
name = "prism"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "dagster",
    "dagster-dbt",
    "dagster-webserver",
    "dbt-duckdb",
    "duckdb",
    "hccpy",
    "httpx",
    "pandas",
    "polars",
    "pydantic>=2.0",
    "pyyaml",
    "rich",
    "tabulate",
    "typer",
]

[project.optional-dependencies]
dev = [
    "commitizen",
    "pre-commit",
    "pytest",
    "pytest-cov",
    "ruff",
]

[project.scripts]
prism = "platform.cli:app"
```

### Installation Commands

```bash
# Initial setup
uv sync                    # Install all deps + create venv

# Add a new dependency
uv add <package>           # Add to [dependencies]
uv add --dev <package>     # Add to [dev-dependencies]

# Update lockfile
uv lock                    # Regenerate uv.lock

# Run commands in venv
uv run pytest              # Run pytest
uv run dagster dev         # Start Dagster
uv run dbt run             # Run dbt models
```

---

## 3. Container Packages

Packages installed inside the container image via Dockerfile.

### Base Image

```dockerfile
FROM python:3.11-slim
```

### Container-Installed Packages

| Package | Install Method | Purpose |
|---------|----------------|---------|
| **uv** | `pip install uv` | Package management inside container |
| **All Python deps** | `uv sync --frozen` | From uv.lock |

### Dockerfile Reference

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies (frozen = use lockfile exactly)
RUN uv sync --frozen

# Copy application code
COPY . .

# Expose Dagster webserver port
EXPOSE 3000

# Default command
CMD ["uv", "run", "dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "platform.definitions"]
```

### docker-compose.yml Reference

```yaml
services:
  dagster:
    build: .
    ports:
      - "3000:3000"
    volumes:
      - ./data:/app/data
      - ./dbt:/app/dbt
    environment:
      - DUCKDB_PATH=/app/data/risk_adjustment.duckdb

  dagster-daemon:
    build: .
    command: uv run dagster-daemon run
    volumes:
      - ./data:/app/data
    depends_on:
      - dagster
```

### Container Commands

```bash
# Build image
podman build -t prism:dev .

# Run with compose
podman-compose up -d

# View logs
podman-compose logs -f dagster

# Stop
podman-compose down
```

---

## 4. GitHub Actions CI/CD

Packages installed in CI runners (ephemeral).

### CI Workflow Dependencies

| Tool | Install Method | Purpose |
|------|----------------|---------|
| **uv** | `astral-sh/setup-uv@v4` | Package management |
| **Python 3.11** | Pre-installed on `ubuntu-latest` | Runtime |
| **Podman** | Pre-installed on `ubuntu-latest` | Container builds |

### CI Workflow Reference

```yaml
name: CI
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync
      - run: uv run ruff check .
      - run: uv run ruff format --check .

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync
      - run: uv run pytest tests/ -v --cov

  dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync
      - run: cd dbt && uv run dbt compile
```

---

## 5. Future Phase 5 (Agents) — Deferred

Additional packages needed when implementing LLM agents:

| Package | Purpose |
|---------|---------|
| **fastapi** | API framework |
| **uvicorn** | ASGI server |
| **langchain** | LLM orchestration |
| **langchain-openai** | OpenAI integration |
| **openai** | OpenAI API client |

These will be added to `pyproject.toml` when Phase 5 is implemented.

---

## 6. Installation Checklist

### First-Time Setup

```bash
# 1. Install OS dependencies
sudo apt update
sudo apt install git podman make python3.11 python3.11-venv

# 2. Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc  # or restart shell

# 3. Clone and setup project
git clone <repo-url> prism
cd prism
uv sync

# 4. Verify installation
uv run pytest tests/ -v
uv run dagster dev  # Should open localhost:3000
```

### Daily Development

```bash
make setup    # Ensure deps are current
make dev      # Start Dagster
make test     # Run tests
make lint     # Check code style
```

---

## 7. Version Pinning Strategy

| Category | Strategy |
|----------|----------|
| **Python** | Pin minor version: `>=3.11,<3.13` |
| **Core deps** | Pin major version in pyproject.toml if stability needed |
| **All deps** | Exact versions locked in `uv.lock` |
| **Container base** | Pin to `python:3.11-slim` (not `latest`) |

### When to Update

- **Weekly**: Run `uv lock --upgrade` in dev, test, merge if green
- **Monthly**: Review major version bumps in dependencies
- **On security advisory**: Immediate patch for affected packages
