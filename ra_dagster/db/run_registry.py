"""
Module: run_registry.py
Description:
    Manages the lifecycle and persistence of run metadata.
    Provides functions to:
    - Allocate group IDs for batched runs.
    - Insert new run records with full configuration context.
    - Update run status (started -> success/failed).

Usage:
    Used by assets (scoring, comparison, decomposition) to track execution history.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ra_dagster.db.bootstrap import now_utc
from ra_dagster.utils.run_ids import GitProvenance, json_dumps


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    run_seq: int
    run_ref: str
    run_timestamp: str
    group_id: int | None
    group_ref: str | None
    group_description: str | None
    run_description: str | None
    analysis_type: str
    calculator: str | None
    model_version: str | None
    benefit_year: int | None
    launchpad_config: dict[str, Any] | None
    blueprint_yml: dict[str, Any]
    git: GitProvenance
    status: str
    trigger_source: str | None
    blueprint_id: str | None
    whoami: str | None
    created_at: datetime
    updated_at: datetime


def allocate_group_id(con: Connection) -> int:
    """Allocate a new group ID for a set of runs."""
    row = con.execute(
        text("SELECT COALESCE(MAX(group_id), 0) + 1 AS next_id FROM main_runs.run_registry")
    ).fetchone()
    return int(row[0])


def allocate_run_seq(con: Connection) -> int:
    """Allocate a new run sequence number."""
    try:
        # Try using the sequence first (DuckDB)
        row = con.execute(text("SELECT nextval('main_runs.run_id_seq')")).fetchone()
        return int(row[0])
    except Exception:
        # Fallback if sequence doesn't exist or not supported
        row = con.execute(
            text("SELECT COALESCE(MAX(run_seq), 0) + 1 AS next_id FROM main_runs.run_registry")
        ).fetchone()
        return int(row[0])


def insert_run(con: Connection, record: RunRecord) -> None:
    """Insert a new run record into the registry."""
    con.execute(
        text("""
        INSERT INTO main_runs.run_registry (
            run_id,
            run_seq,
            run_ref,
            run_timestamp,
            status,
            analysis_type,
            run_description,
            group_id,
            group_ref,
            group_description,
            calculator,
            model_version,
            benefit_year,
            launchpad_config,
            created_at,
            updated_at,
            trigger_source,
            git_branch,
            git_commit,
            git_commit_short,
            git_commit_clean,
            blueprint_id,
            blueprint_yml,
            whoami
        ) VALUES (
            :run_id,
            :run_seq,
            :run_ref,
            :run_timestamp,
            :status,
            :analysis_type,
            :run_description,
            :group_id,
            :group_ref,
            :group_description,
            :calculator,
            :model_version,
            :benefit_year,
            :launchpad_config,
            :created_at,
            :updated_at,
            :trigger_source,
            :git_branch,
            :git_commit,
            :git_commit_short,
            :git_commit_clean,
            :blueprint_id,
            :blueprint_yml,
            :whoami
        )
        """),
        {
            "run_id": record.run_id,
            "run_seq": record.run_seq,
            "run_ref": record.run_ref,
            "run_timestamp": record.run_timestamp,
            "status": record.status,
            "analysis_type": record.analysis_type,
            "run_description": record.run_description,
            "group_id": record.group_id,
            "group_ref": record.group_ref,
            "group_description": record.group_description,
            "calculator": record.calculator,
            "model_version": record.model_version,
            "benefit_year": record.benefit_year,
            "launchpad_config": json_dumps(record.launchpad_config)
            if record.launchpad_config is not None
            else None,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "trigger_source": record.trigger_source,
            "git_branch": record.git.branch,
            "git_commit": record.git.commit,
            "git_commit_short": record.git.commit_short,
            "git_commit_clean": record.git.clean,
            "blueprint_id": record.blueprint_id,
            "blueprint_yml": json_dumps(record.blueprint_yml),
            "whoami": record.whoami,
        },
    )


def update_run_status(
    con: Connection,
    *,
    run_id: str,
    status: str,
) -> None:
    """Update the status of an existing run."""
    con.execute(
        text("""
        UPDATE main_runs.run_registry
        SET status = :status, updated_at = :updated_at
        WHERE run_id = :run_id
        """),
        {"status": status, "updated_at": now_utc(), "run_id": run_id},
    )


def resolve_run_id(con: Connection, run_identifier: str) -> str:
    """
    Resolves a run identifier to a UUID.
    If the input is a valid UUID string, returns it as is.
    Otherwise, assumes it's a run_ref (e.g., 's00015') and queries the registry.
    """
    # Simple heuristic: if it looks like a long UUID, return it
    if len(run_identifier) > 20 and "-" in run_identifier:
        return run_identifier

    # Otherwise treat as ref
    row = con.execute(
        text("SELECT run_id FROM main_runs.run_registry WHERE run_ref = :code"),
        {"code": run_identifier.lower()},
    ).fetchone()

    if not row:
        raise ValueError(f"Run ref '{run_identifier}' not found in registry.")

    return row[0]
