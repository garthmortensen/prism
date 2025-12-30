from __future__ import annotations

from pathlib import Path

from dagster import AssetExecutionContext, asset

from ra_dagster.db.bootstrap import ensure_prism_warehouse, now_utc
from ra_dagster.db.run_registry import (
    RunRecord,
    allocate_group_id,
    insert_run,
    update_run_status,
)
from ra_dagster.resources.duckdb_resource import DuckDBResource
from ra_dagster.utils.run_ids import (
    generate_run_id,
    generate_run_timestamp,
    get_git_provenance,
    json_dumps,
)


@asset
def compare_runs(context, duckdb: DuckDBResource) -> None:
    """Compute member-level deltas between two scoring runs into main_analytics.run_comparison."""

    config = context.op_config or {}
    run_id_a = config.get("run_id_a")
    run_id_b = config.get("run_id_b")
    run_timestamp_a = config.get("run_timestamp_a")
    run_timestamp_b = config.get("run_timestamp_b")

    if not (run_id_a and run_id_b) and not (run_timestamp_a and run_timestamp_b):
        raise ValueError(
            "compare_runs requires op config: run_id_a/run_id_b (preferred) or "
            "run_timestamp_a/run_timestamp_b (legacy)"
        )

    con = duckdb.get_connection().connect()

    ensure_prism_warehouse(con)

    if not (run_id_a and run_id_b):
        ids_a = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT run_id FROM main_runs.risk_scores WHERE run_timestamp = ?",
                [run_timestamp_a],
            ).fetchall()
        ]
        ids_b = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT run_id FROM main_runs.risk_scores WHERE run_timestamp = ?",
                [run_timestamp_b],
            ).fetchall()
        ]

        if len(ids_a) != 1 or len(ids_b) != 1:
            raise ValueError(
                "run_timestamp_* must map to exactly one run_id. "
                "Pass run_id_a/run_id_b to disambiguate."
            )

        run_id_a = ids_a[0]
        run_id_b = ids_b[0]

    run_id = generate_run_id()
    run_ts = generate_run_timestamp()
    git = get_git_provenance(cwd=str(Path(__file__).resolve().parents[2]))

    group_id = config.get("group_id")
    if group_id is None:
        group_id = allocate_group_id(con)

    record = RunRecord(
        run_id=run_id,
        run_timestamp=run_ts,
        group_id=int(group_id),
        group_description=config.get("group_description"),
        run_description=config.get(
            "run_description",
            f"Compare runs {run_timestamp_a} vs {run_timestamp_b}",
        ),
        analysis_type="comparison",
        calculator=None,
        model_version=None,
        benefit_year=None,
        data_effective=None,
        json_config={
            "run_timestamp_a": run_timestamp_a,
            "run_timestamp_b": run_timestamp_b,
            **config,
        },
        git=git,
        status="started",
        trigger_source=config.get("trigger_source", "dagster"),
        created_at=now_utc(),
        updated_at=now_utc(),
    )

    insert_run(con, record)

    try:
        created_at = now_utc()

        con.execute(
            """
            INSERT INTO main_analytics.run_comparison (
                run_id_a,
                run_id_b,
                member_id,
                match_status,
                score_a,
                score_b,
                score_diff,
                details,
                created_at
            )
            SELECT
                ?,
                ?,
                COALESCE(a.member_id, b.member_id) AS member_id,
                CASE
                    WHEN a.member_id IS NOT NULL AND b.member_id IS NOT NULL THEN 'matched'
                    WHEN a.member_id IS NOT NULL AND b.member_id IS NULL THEN 'a_only'
                    ELSE 'b_only'
                END AS match_status,
                a.risk_score AS score_a,
                b.risk_score AS score_b,
                CASE
                    WHEN a.risk_score IS NOT NULL AND b.risk_score IS NOT NULL THEN (
                        b.risk_score - a.risk_score
                    )
                    ELSE NULL
                END AS score_diff,
                CAST(? AS JSON) AS details,
                ?
                FROM (
                    SELECT * FROM main_runs.risk_scores WHERE run_id = ?
                ) a
                FULL OUTER JOIN (
                    SELECT * FROM main_runs.risk_scores WHERE run_id = ?
                ) b
                  ON a.member_id = b.member_id
            """,
            [
                str(run_id_a),
                str(run_id_b),
                json_dumps(
                    {
                        "run_id_a": run_id_a,
                        "run_id_b": run_id_b,
                    }
                ),
                created_at,
                str(run_id_a),
                str(run_id_b),
            ],
        )

        update_run_status(con, run_id=run_id, status="success")
        context.log.info(f"Wrote run comparison for run_id_a={run_id_a}, run_id_b={run_id_b}")

    except Exception:
        update_run_status(con, run_id=run_id, status="failed")
        raise

    finally:
        con.close()
