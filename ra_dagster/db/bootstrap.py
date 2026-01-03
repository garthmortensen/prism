from __future__ import annotations

from datetime import datetime

import duckdb


def _risk_scores_details_components_last(con: duckdb.DuckDBPyConnection) -> bool:
    rows = con.execute("PRAGMA table_info('main_runs.risk_scores')").fetchall()
    # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
    cols = [r[1] for r in rows]
    if len(cols) < 2:
        return True
    return cols[-2:] == ["details", "components"]


def _recreate_risk_scores_with_details_components_last(con: duckdb.DuckDBPyConnection) -> None:
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

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DROP TABLE IF EXISTS {tmp}")
        con.execute(
            f"""
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
            """
        )

        cols_sql = ", ".join(ordered_cols)
        con.execute(f"INSERT INTO {tmp} ({cols_sql}) SELECT {cols_sql} FROM main_runs.risk_scores")

        con.execute("DROP TABLE main_runs.risk_scores")
        con.execute("ALTER TABLE main_runs.risk_scores__tmp_reorder RENAME TO risk_scores")

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


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
            launchpad_config VARCHAR,
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

    # Backfill columns for warehouses created before these fields were added.
    con.execute(
        "ALTER TABLE main_runs.run_registry ADD COLUMN IF NOT EXISTS launchpad_config VARCHAR"
    )

    # Add index on run_timestamp for sorting (not unique to allow sub-second collisions)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_run_registry_timestamp ON main_runs.run_registry "
        "(run_timestamp)"
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

    # If this warehouse existed before we standardized column ordering,
    # details/components may not be physically last. Recreate table once to reorder.
    _recreate_risk_scores_with_details_components_last(con)

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
