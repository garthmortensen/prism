"""
Script: bootstrap.py
Description:
    Ensures the 'prism' warehouse schema exists and is up-to-date.

    This module serves as the Schema Definition Layer for the SQLAlchemy migration.
    It is essential for:
    1. Cross-Database Compatibility: Abstracts DDL differences (e.g., Snowflake VARIANT vs DuckDB JSON).
    2. Centralized Source of Truth: Defines official table structures (risk_scores, run_registry) in one place.
    3. Environment Initialization: Allows consistent setup of both 'dev' (DuckDB) and 'prod' (Snowflake) environments.

    Handles dialect-specific DDL for:
    - Run Registry (tracking jobs)
    - Risk Scores (output results)
    - Input Views (data staging)

Usage:
    Called automatically by Dagster resources or manually via `ra_dagster init-db`.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    inspect,
    text,
)
from sqlalchemy.engine import Connection
from sqlalchemy.types import JSON, TIMESTAMP



def ensure_core_schemas(con: Connection) -> None:
    """Ensure that the core schemas exist in the database."""
    con.execute(text("CREATE SCHEMA IF NOT EXISTS dag_runs"))
    con.execute(text("CREATE SCHEMA IF NOT EXISTS dag_analytics"))


def ensure_run_registry(con: Connection) -> None:
    """Ensure that the run_registry table exists."""
    
    con.execute(text("CREATE SEQUENCE IF NOT EXISTS dag_runs.run_id_seq START 1"))

    metadata = MetaData(schema="dag_runs")
    Table(
        "run_registry",
        metadata,
        Column("run_id", String, primary_key=True),
        Column("run_seq", Integer),
        Column("run_ref", String),
        Column("run_timestamp", String),
        Column("group_id", Integer),
        Column("group_ref", String),
        Column("group_description", String),
        Column("run_description", String),
        Column("analysis_type", String),
        Column("calculator", String),
        Column("model_version", String),
        Column("benefit_year", Integer),
        Column("data_effective", String),
        Column("launchpad_config", String),
        Column("blueprint_yml", String),
        Column("git_branch", String),
        Column("git_commit", String),
        Column("git_commit_short", String),
        Column("git_commit_clean", Boolean),
        Column("status", String),
        Column("trigger_source", String),
        Column("blueprint_id", String),
        Column("whoami", String),
        Column("created_at", TIMESTAMP),
        Column("updated_at", TIMESTAMP),
    )
    metadata.create_all(con)

    # Backfill columns for warehouses created before these fields were added.
    con.execute(
        text(
            "ALTER TABLE dag_runs.run_registry ADD COLUMN IF NOT EXISTS launchpad_config VARCHAR"
        )
    )
    con.execute(text("ALTER TABLE dag_runs.run_registry ADD COLUMN IF NOT EXISTS whoami VARCHAR"))

    # Add index on run_timestamp for sorting (not unique to allow sub-second collisions)
    con.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_run_registry_timestamp ON dag_runs.run_registry "
            "(run_timestamp)"
        )
    )


def ensure_marts_tables(con: Connection) -> None:
    """Ensure that the data marts tables exist."""
    metadata = MetaData()

    Table(
        "risk_scores",
        metadata,
        Column("run_id", String, primary_key=True),
        Column("member_id", String, primary_key=True),
        Column("risk_score", Float),
        Column("hcc_score", Float),
        Column("rxc_score", Float),
        Column("demographic_score", Float),
        Column("model", String),
        Column("gender", String),
        Column("metal_level", String),
        Column("enrollment_months", Integer),
        Column("model_year", String),
        Column("benefit_year", Integer),
        Column("calculator", String),
        Column("model_version", String),
        Column("run_timestamp", String),
        Column("created_at", TIMESTAMP),
        Column("hcc_list", JSON),
        Column("rxc_list", JSON),
        Column("details", JSON),
        Column("components", JSON),
        schema="dag_runs",
    )

    Table(
        "run_comparison",
        metadata,
        Column("batch_id", String, primary_key=True),
        Column("run_id_a", String),
        Column("run_id_b", String),
        Column("member_id", String, primary_key=True),
        Column("match_status", String),
        Column("score_a", Float),
        Column("score_b", Float),
        Column("score_diff", Float),
        Column("details", JSON),
        Column("created_at", TIMESTAMP),
        schema="dag_analytics",
    )

    Table(
        "decomposition_scenarios",
        metadata,
        Column("batch_id", String, primary_key=True),
        Column("driver_name", String, primary_key=True),
        Column("impact_value", Float),
        Column("run_id", String),
        Column("created_at", TIMESTAMP),
        schema="dag_analytics",
    )

    Table(
        "decomposition_definitions",
        metadata,
        Column("batch_id", String, primary_key=True),
        Column("step_index", Integer, primary_key=True),
        Column("driver_name", String),
        Column("description", String),
        Column("created_at", TIMESTAMP),
        schema="dag_analytics",
    )

    metadata.create_all(con)


def ensure_prism_warehouse(con: Connection) -> None:
    """Ensure that the entire Prism warehouse structure exists."""
    ensure_core_schemas(con)
    ensure_run_registry(con)
    ensure_marts_tables(con)


def now_utc() -> datetime:
    """Get the current UTC timestamp."""
    return datetime.utcnow()
