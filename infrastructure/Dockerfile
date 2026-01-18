# Prism - Risk Adjustment Analytics Lab
# Podman/Docker compatible container image
# PostgreSQL-backed risk adjustment analytics platform

FROM python:3.11-slim

# Labels
LABEL maintainer="prism"
LABEL description="Risk adjustment analytics platform with PostgreSQL"
LABEL version="1.0.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DAGSTER_HOME=/app/.dagster_home \
    DATABASE_URL=postgresql://ra_user:ra_pass@postgres:5432/ra_database

WORKDIR /app

# Install system dependencies
# PostgreSQL client libraries needed for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock* README.md ./

# Install dependencies with all extras (includes dev tools)
RUN uv sync --frozen || uv sync

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p \
    /app/.dagster_home \
    /app/logs \
    /app/output \
    /app/ra_dagster/storage

# Copy Dagster configuration
COPY dagster_home/dagster.yaml /app/.dagster_home/dagster.yaml

# Expose Dagster webserver port
EXPOSE 3000

# Health check for Dagster webserver
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

# Default command - start Dagster dev server
CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "ra_dagster.definitions"]
