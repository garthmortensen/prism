"""
Resource: sqlalchemy_resource.py
Description:
    Provides a unified SQLAlchemy engine for database access.
    - Abstracts connection details for DuckDB (Dev) and Snowflake (Prod).
    - Manages environment-specific configuration via `DATABASE_URL`.

Usage:
    Injected into assets to provide `database.get_engine()`.
"""
from pathlib import Path
from dagster import resource
from sqlalchemy import create_engine, Engine
import os

class SqlAlchemyResource:
    def __init__(self, database: str):
        self.database = database

    def get_engine(self) -> Engine:
        if self.database == "dev":
            url = os.getenv("DATABASE_URL")
            if not url:
                # Fallback to local duckdb
                db_path = Path(__file__).resolve().parents[2] / "risk_adjustment.duckdb"
                url = f"duckdb:///{db_path}"
            
            # Use NullPool for DuckDB to avoid holding locks
            from sqlalchemy.pool import NullPool
            return create_engine(url, poolclass=NullPool)
        elif self.database == "prod":
            url = os.getenv("DATABASE_URL")
            if not url:
                raise ValueError("DATABASE_URL environment variable must be set for prod (Snowflake)")
            return create_engine(url)
        else:
            raise ValueError(f"Unknown database environment: {self.database}")

@resource(config_schema=str)
def db_resource(init_context):
    return SqlAlchemyResource(database=init_context.resource_config)
