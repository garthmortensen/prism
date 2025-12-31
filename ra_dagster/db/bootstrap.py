from __future__ import annotations

from datetime import datetime

import duckdb


def ensure_core_schemas(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS main_intermediate")
    con.execute("CREATE SCHEMA IF NOT EXISTS main_runs")
    con.execute("CREATE SCHEMA IF NOT EXISTS main_analytics")


def ensure_run_registry(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main_runs.run_registry (
            run_id VARCHAR PRIMARY KEY,
            run_timestamp VARCHAR,
            group_id BIGINT,
            group_description VARCHAR,
            run_description VARCHAR,
            analysis_type VARCHAR,
            calculator VARCHAR,
            model_version VARCHAR,
            benefit_year INTEGER,
            data_effective VARCHAR,
            blueprint_yml VARCHAR,
            git_branch VARCHAR,
            git_commit VARCHAR,
            git_commit_short VARCHAR,
            git_commit_clean BOOLEAN,
            status VARCHAR,
            trigger_source VARCHAR,
            blueprint_id VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
    )

    # Add index on run_timestamp for sorting (not unique to allow sub-second collisions)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_run_registry_timestamp ON main_runs.run_registry (run_timestamp)"
    )


def ensure_marts_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main_runs.risk_scores (
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
        """
    )

    # Backfill columns for warehouses created before these fields were added.
    con.execute("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS model_year VARCHAR")
    con.execute("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS components JSON")
    con.execute("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS gender VARCHAR")
    con.execute("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS model VARCHAR")
    con.execute("ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS metal_level VARCHAR")
    con.execute(
        "ALTER TABLE main_runs.risk_scores ADD COLUMN IF NOT EXISTS enrollment_months INTEGER"
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main_analytics.run_comparison (
            batch_id VARCHAR,
            run_id_a VARCHAR,
            run_id_b VARCHAR,
            member_id VARCHAR,
            score_diff DOUBLE,
            match_status VARCHAR,
            score_a DOUBLE,
            score_b DOUBLE,
            created_at TIMESTAMP,
            details JSON
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main_analytics.run_comparison (
            batch_id VARCHAR,
            run_id_a VARCHAR,
            run_id_b VARCHAR,
            member_id VARCHAR,
            match_status VARCHAR,
            score_a DOUBLE,
            score_b DOUBLE,
            score_diff DOUBLE,
            details JSON,
            created_at TIMESTAMP,
            PRIMARY KEY (batch_id, member_id)
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main_analytics.decomposition_scenarios (
            batch_id VARCHAR,
            driver_name VARCHAR,
            impact_value DOUBLE,
            run_id VARCHAR,
            created_at TIMESTAMP,
            PRIMARY KEY (batch_id, driver_name)
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main_analytics.decomposition_definitions (
            batch_id VARCHAR,
            step_index INTEGER,
            driver_name VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP,
            PRIMARY KEY (batch_id, step_index)
        )
        """
    )


def ensure_prism_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    ensure_core_schemas(con)
    ensure_run_registry(con)
    ensure_marts_tables(con)


def now_utc() -> datetime:
    return datetime.utcnow()
