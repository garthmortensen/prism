"""
Resource: sqlalchemy_resource.py
Description:
    Provides a unified SQLAlchemy engine for database access.
    - Abstracts connection details for PostgreSQL (Dev) and Snowflake (Prod).
    - Manages environment-specific configuration via `DATABASE_URL`.

Usage:
    Injected into assets to provide `database.get_engine()`.
"""
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
                # Fallback to local PostgreSQL (Docker)
                url = "postgresql://ra_user:ra_pass@localhost:5432/ra_database"
            return create_engine(url)
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
