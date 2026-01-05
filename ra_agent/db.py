import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Uses SQLAlchemy so we can point to DuckDB (dev) or Snowflake (prod)
DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///risk_adjustment.duckdb")

# Use NullPool to ensure connections are closed immediately, releasing locks
# This is critical for DuckDB when triggering external processes (like Dagster jobs)
engine = create_engine(DATABASE_URL, poolclass=NullPool)
