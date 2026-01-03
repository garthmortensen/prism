# Prism - Risk Adjustment Analytics Lab
# Podman/Docker compatible container image

FROM python:3.11-slim

# Labels
LABEL maintainer="prism"
LABEL description="Risk adjustment analytics platform"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock* ./

# Install dependencies (frozen = use lockfile exactly)
# If uv.lock doesn't exist, this will create it
RUN uv sync --frozen --no-dev || uv sync --no-dev

# Copy application code
COPY . .

# Create data directory for DuckDB
RUN mkdir -p /app/data

# Expose Dagster webserver port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

# Default command - start Dagster webserver
CMD ["uv", "run", "dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "ra_dagster.definitions"]
