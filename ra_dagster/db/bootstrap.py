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


def _is_duckdb(con: Connection) -> bool:
    return con.dialect.name == "duckdb"


def _get_json_type(con: Connection) -> str:
    if con.dialect.name == "snowflake":
        return "VARIANT"
    return "JSON"


def _risk_scores_details_components_last(con: Connection) -> bool:
    """Get the schema for the risk_scores table with details and components at the end."""
    if not _is_duckdb(con):
        return True

    try:
        insp = inspect(con)
        cols = insp.get_columns("risk_scores", schema="main_runs")
        if not cols:
            return True
        col_names = [c["name"] for c in cols]
        if len(col_names) < 2:
            return True
        return col_names[-2:] == ["details", "components"]
    except Exception:
        # Fallback or error handling
        return True


def _recreate_risk_scores_with_details_components_last(con: Connection) -> None:
    """Recreate the risk_scores table with the updated schema."""
    if not _is_duckdb(con):
        return

    # Keep this migration narrow: only reorder when details/components aren't last.
    if _risk_scores_details_components_last(con):
        return

    tmp = "main_runs.risk_scores__tmp_reorder"

    # Desired physical order (details/components at end)
    ordered_cols = [
        "run_id",
        "member_id",
        "risk_score",
        "hcc_score",
        "rxc_score",
        "demographic_score",
        "model",
        "gender",
        "metal_level",
        "enrollment_months",
        "model_year",
        "benefit_year",
        "calculator",
        "model_version",
        "run_timestamp",
        "created_at",
        "hcc_list",
        "rxc_list",
        "details",
        "components",
    ]

    with con.begin():
        con.execute(text(f"DROP TABLE IF EXISTS {tmp}"))
        con.execute(
            text(f"""
            CREATE TABLE {tmp} (
                run_id VARCHAR,
                member_id VARCHAR,
                risk_score DOUBLE,
                hcc_score DOUBLE,
                rxc_score DOUBLE,
                demographic_score DOUBLE,
                model VARCHAR,
                gender VARCHAR,
                metal_level VARCHAR,
                enrollment_months INTEGER,
                model_year VARCHAR,
                benefit_year INTEGER,
                calculator VARCHAR,
                model_version VARCHAR,
                run_timestamp VARCHAR,
                created_at TIMESTAMP,
                hcc_list JSON,
                rxc_list JSON,
                details JSON,
                components JSON,
                PRIMARY KEY (run_id, member_id)
            )
            """)
        )

        cols_sql = ", ".join(ordered_cols)
        con.execute(
            text(f"INSERT INTO {tmp} ({cols_sql}) SELECT {cols_sql} FROM main_runs.risk_scores")
        )

        con.execute(text("DROP TABLE main_runs.risk_scores"))
        con.execute(text("ALTER TABLE main_runs.risk_scores__tmp_reorder RENAME TO risk_scores"))


def ensure_core_schemas(con: Connection) -> None:
    """Ensure that the core schemas exist in the database."""
    con.execute(text("CREATE SCHEMA IF NOT EXISTS main_intermediate"))
    con.execute(text("CREATE SCHEMA IF NOT EXISTS main_runs"))
    con.execute(text("CREATE SCHEMA IF NOT EXISTS main_analytics"))


def ensure_run_registry(con: Connection) -> None:
    """Ensure that the run_registry table exists."""
    
    if _is_duckdb(con):
        con.execute(text("CREATE SEQUENCE IF NOT EXISTS main_runs.run_id_seq START 1"))

    metadata = MetaData(schema="main_runs")
    Table(
        "run_registry",
        metadata,
        Column("run_id", String, primary_key=True),
        Column("run_seq", Integer),
        Column("run_code", String),
        Column("run_timestamp", String),
        Column("group_id", Integer),
        Column("group_code", String),
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
        Column("created_at", TIMESTAMP),
        Column("updated_at", TIMESTAMP),
    )
    metadata.create_all(con)

    # Backfill columns for warehouses created before these fields were added.
    con.execute(
        text(
            "ALTER TABLE main_runs.run_registry ADD COLUMN IF NOT EXISTS launchpad_config VARCHAR"
        )
    )

    # Add index on run_timestamp for sorting (not unique to allow sub-second collisions)
    con.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_run_registry_timestamp ON main_runs.run_registry "
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
        schema="main_runs",
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
        schema="main_analytics",
    )

    Table(
        "decomposition_scenarios",
        metadata,
        Column("batch_id", String, primary_key=True),
        Column("driver_name", String, primary_key=True),
        Column("impact_value", Float),
        Column("run_id", String),
        Column("created_at", TIMESTAMP),
        schema="main_analytics",
    )

    Table(
        "decomposition_definitions",
        metadata,
        Column("batch_id", String, primary_key=True),
        Column("step_index", Integer, primary_key=True),
        Column("driver_name", String),
        Column("description", String),
        Column("created_at", TIMESTAMP),
        schema="main_analytics",
    )

    metadata.create_all(con)

    json_type = _get_json_type(con)

    # Backfill columns for warehouses created before these fields were added.
    con.execute(text("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS model_year VARCHAR"))
    con.execute(
        text(f"ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS components {json_type}")
    )
    con.execute(text("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS gender VARCHAR"))
    con.execute(text("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS model VARCHAR"))
    con.execute(text("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS metal_level VARCHAR"))
    con.execute(
        text(
            "ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS enrollment_months INTEGER"
        )
    )

    # If this warehouse existed before we standardized column ordering,
    # details/components may not be physically last. Recreate table once to reorder.
    _recreate_risk_scores_with_details_components_last(con)


def ensure_prism_warehouse(con: Connection) -> None:
    """Ensure that the entire Prism warehouse structure exists."""
    ensure_core_schemas(con)
    ensure_run_registry(con)
    ensure_marts_tables(con)


def now_utc() -> datetime:
    """Get the current UTC timestamp."""
    return datetime.utcnow()
