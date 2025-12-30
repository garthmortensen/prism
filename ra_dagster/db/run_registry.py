from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import duckdb

from ra_dagster.db.bootstrap import now_utc
from ra_dagster.utils.run_ids import GitProvenance, json_dumps


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    run_timestamp: str
    group_id: int | None
    group_description: str | None
    run_description: str | None
    analysis_type: str
    calculator: str | None
    model_version: str | None
    benefit_year: int | None
    data_effective: str | None
    json_config: dict[str, Any]
    git: GitProvenance
    status: str
    trigger_source: str | None
    created_at: datetime
    updated_at: datetime


def allocate_group_id(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute(
        "SELECT COALESCE(MAX(group_id), 0) + 1 AS next_id FROM main_runs.run_registry"
    ).fetchone()
    return int(row[0])


def insert_run(con: duckdb.DuckDBPyConnection, record: RunRecord) -> None:
    con.execute(
        """
        INSERT INTO main_runs.run_registry (
            run_id,
            run_timestamp,
            group_id,
            group_description,
            run_description,
            analysis_type,
            calculator,
            model_version,
            benefit_year,
            data_effective,
            json_config,
            git_branch,
            git_commit,
            git_commit_short,
            git_commit_clean,
            status,
            trigger_source,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            record.run_id,
            record.run_timestamp,
            record.group_id,
            record.group_description,
            record.run_description,
            record.analysis_type,
            record.calculator,
            record.model_version,
            record.benefit_year,
            record.data_effective,
            json_dumps(record.json_config),
            record.git.branch,
            record.git.commit,
            record.git.commit_short,
            record.git.clean,
            record.status,
            record.trigger_source,
            record.created_at,
            record.updated_at,
        ],
    )


def update_run_status(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    status: str,
) -> None:
    con.execute(
        """
        UPDATE main_runs.run_registry
        SET status = ?, updated_at = ?
        WHERE run_id = ?
        """,
        [status, now_utc(), run_id],
    )
