from __future__ import annotations

from pathlib import Path

from dagster import asset

from ra_dagster.db.bootstrap import ensure_prism_warehouse, now_utc
from ra_dagster.db.run_registry import (
    RunRecord,
    allocate_group_id,
    insert_run,
    update_run_status,
)
from ra_dagster.resources.duckdb_resource import DuckDBResource
from ra_dagster.utils.run_ids import (
    extract_launchpad_config,
    generate_run_timestamp,
    get_git_provenance,
    json_dumps,
)


@asset
def compare_runs(context, duckdb: DuckDBResource) -> None:
    """
    Compute member-level deltas between two scoring runs into main_analytics.run_comparison.

    Config:
        run_id_a: str
        run_id_b: str
        metric: str = "mean" | "sum" (default: "mean")
        metric: str = "mean" | "sum" (default: "mean")
        population_mode: str = "intersection" | "union" | "a_only" | "b_only"
            (default: "intersection")
    """
    config = context.op_config or {}
    run_id_a = config.get("run_id_a")
    run_id_b = config.get("run_id_b")
    metric = config.get("metric", "mean")
    population_mode = config.get("population_mode", "intersection")

    if not (run_id_a and run_id_b):
        raise ValueError("compare_runs requires op config: run_id_a and run_id_b")

    con = duckdb.get_connection().connect()

    ensure_prism_warehouse(con)

    run_id = context.run_id
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
            f"Compare runs {run_id_a} vs {run_id_b}",
        ),
        analysis_type="comparison",
        calculator=None,
        model_version=None,
        benefit_year=None,
        data_effective=None,
        launchpad_config=extract_launchpad_config(context=context, fallback=config),
        blueprint_yml={
            "run_id_a": run_id_a,
            "run_id_b": run_id_b,
            "metric": metric,
            "population_mode": population_mode,
            **config,
        },
        git=git,
        status="started",
        trigger_source=config.get("trigger_source", "dagster"),
        blueprint_id=str(config.get("blueprint_id"))
        if config.get("blueprint_id") is not None
        else None,
        created_at=now_utc(),
        updated_at=now_utc(),
    )

    insert_run(con, record)

    try:
        # Determine Join Type based on population_mode
        if population_mode == "intersection":
            join_type = "INNER JOIN"
        elif population_mode == "union":
            join_type = "FULL OUTER JOIN"
        elif population_mode == "a_only":
            join_type = "LEFT JOIN"
        elif population_mode == "b_only":
            join_type = "RIGHT JOIN"
        else:
            raise ValueError(f"Unknown population_mode: {population_mode}")

        # batch_id is the unique ID for this execution (using run_id)
        batch_id = context.run_id

        con.execute(
            f"""
            INSERT INTO main_analytics.run_comparison (
                batch_id,
                run_id_a,
                run_id_b,
                member_id,
                score_diff,
                match_status,
                score_a,
                score_b,
                created_at,
                details
            )
            WITH A AS (SELECT member_id, risk_score FROM main_runs.risk_scores WHERE run_id = ?),
                 B AS (SELECT member_id, risk_score FROM main_runs.risk_scores WHERE run_id = ?)
            SELECT
                ?,
                ?,
                ?,
                COALESCE(A.member_id, B.member_id) as member_id,
                COALESCE(B.risk_score, 0.0) - COALESCE(A.risk_score, 0.0) as score_diff,
                CASE
                    WHEN A.member_id IS NOT NULL AND B.member_id IS NOT NULL THEN 'matched'
                    WHEN A.member_id IS NOT NULL THEN 'a_only'
                    WHEN B.member_id IS NOT NULL THEN 'b_only'
                END as match_status,
                COALESCE(A.risk_score, 0.0) as score_a,
                COALESCE(B.risk_score, 0.0) as score_b,
                ?,
                CAST(? AS JSON) as details
            FROM A
            {join_type} B ON A.member_id = B.member_id
            """,
            [
                run_id_a,
                run_id_b,
                batch_id,
                run_id_a,
                run_id_b,
                now_utc(),
                json_dumps({}),
            ],
        )

        update_run_status(con, run_id=run_id, status="success")
        context.log.info(f"Wrote main_analytics.run_comparison for batch_id={batch_id}")

    except Exception:
        update_run_status(con, run_id=run_id, status="failed")
        raise

    finally:
        con.close()
