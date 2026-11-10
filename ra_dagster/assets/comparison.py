"""
Asset: comparison.py
Description:
    Compares two distinct scoring runs to identify drivers of change.
    - Calculates member-level score differences.
    - Categorizes matches (matched, a_only, b_only).
    - Supports different population modes (intersection, union, etc.).

Usage:
    Executed via the `comparison_job` in Dagster.
"""

import getpass
from pathlib import Path

from dagster import AssetExecutionContext, asset, ResourceParam
from sqlalchemy import text

from ra_dagster.config_schemas import ComparisonConfig
from ra_dagster.db.bootstrap import ensure_prism_warehouse, now_utc
from ra_dagster.db.run_registry import (
    RunRecord,
    allocate_group_id,
    allocate_run_seq,
    insert_run,
    update_run_status,
    resolve_run_id,
)
from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource
from ra_dagster.utils.run_ids import (
    extract_launchpad_config,
    generate_run_timestamp,
    get_git_provenance,
    json_dumps,
)
from ra_dagster.utils.run_refs import generate_run_ref


@asset
def compare_runs(context: AssetExecutionContext, config: ComparisonConfig, database: ResourceParam[SqlAlchemyResource]) -> None:
    """
    Compute member-level deltas between two scoring runs.
    """
    run_ref_a_raw = config.run_ref_a
    run_ref_b_raw = config.run_ref_b
    metric = config.metric
    population_mode = config.population_mode

    if not (run_ref_a_raw and run_ref_b_raw):
        raise ValueError("compare_runs requires op config: run_ref_a and run_ref_b")


    engine = database.get_engine()
    con = engine.connect()

    ensure_prism_warehouse(con)

    # Resolve human-friendly codes to UUIDs if necessary
    run_id_a = resolve_run_id(con, run_ref_a_raw)
    run_id_b = resolve_run_id(con, run_ref_b_raw)

    run_id = context.run_id
    run_ts = generate_run_timestamp()
    git = get_git_provenance(cwd=str(Path(__file__).resolve().parents[2]))

    group_id = config.group_id
    if group_id is None:
        group_id = allocate_group_id(con)

    run_seq = allocate_run_seq(con)
    run_ref = generate_run_ref(run_seq, width=4, prefix="c")
    group_ref = (
        generate_run_ref(int(group_id), width=4, prefix="b")
        if group_id is not None
        else None
    )

    context.log.info(f"Run Ref: {run_ref}")
    if group_ref:
        context.log.info(f"Batch Ref: {group_ref}")

    record = RunRecord(
        run_id=run_id,
        run_seq=run_seq,
        run_ref=run_ref,
        run_timestamp=run_ts,
        group_id=int(group_id),
        group_ref=group_ref,
        group_description=config.group_description,
        run_description=config.run_description or f"Compare runs {run_id_a} vs {run_id_b}",
        analysis_type="comparison",
        calculator=None,
        model_version=None,
        benefit_year=None,
        launchpad_config=extract_launchpad_config(
            context=context,
            fallback={
                "ops": {
                    "compare_runs": {
                        "config": config.model_dump(),
                    }
                }
            },
        ),
        blueprint_yml={
            "run_id_actual": run_id,  # store the generated UUID inside the blueprint for downstream use
            **config.model_dump(),
        },
        git=git,
        status="started",
        trigger_source="dagster",
        blueprint_id=None,
        whoami=getpass.getuser(),
        created_at=now_utc(),
        updated_at=now_utc(),
    )

    insert_run(con, record)
    con.commit()

    try:
        # Determine Join Type based on population_mode
        if population_mode.value == "intersection":
            join_type = "INNER JOIN"
        elif population_mode.value == "union":
            join_type = "FULL OUTER JOIN"
        elif population_mode.value == "a_only":
            join_type = "LEFT JOIN"
        elif population_mode.value == "b_only":
            join_type = "RIGHT JOIN"
        else:
            raise ValueError(f"Unknown population_mode: {population_mode.value}")

        # batch_id is the unique ID for this execution (using run_id)
        batch_id = context.run_id

        json_cast = "CAST(:details AS JSON)"
        if engine.dialect.name == "snowflake":
            json_cast = "PARSE_JSON(:details)"

        con.execute(
            text(f"""
            INSERT INTO dag_analytics.run_comparison (
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
            WITH A AS (SELECT member_id, risk_score FROM dag_runs.risk_scores WHERE run_id = :run_id_a_filter),
                 B AS (SELECT member_id, risk_score FROM dag_runs.risk_scores WHERE run_id = :run_id_b_filter)
            SELECT
                :batch_id,
                :run_id_a,
                :run_id_b,
                COALESCE(A.member_id, B.member_id) as member_id,
                COALESCE(B.risk_score, 0.0) - COALESCE(A.risk_score, 0.0) as score_diff,
                CASE
                    WHEN A.member_id IS NOT NULL AND B.member_id IS NOT NULL THEN 'matched'
                    WHEN A.member_id IS NOT NULL THEN 'a_only'
                    WHEN B.member_id IS NOT NULL THEN 'b_only'
                END as match_status,
                COALESCE(A.risk_score, 0.0) as score_a,
                COALESCE(B.risk_score, 0.0) as score_b,
                :created_at,
                {json_cast} as details
            FROM A
            {join_type} B ON A.member_id = B.member_id
            """),
            {
                "run_id_a_filter": run_id_a,
                "run_id_b_filter": run_id_b,
                "batch_id": batch_id,
                "run_id_a": run_id_a,
                "run_id_b": run_id_b,
                "created_at": now_utc(),
                "details": json_dumps({}),
            },
        )

        update_run_status(con, run_id=run_id, status="success")
        con.commit()
        context.log.info(f"Wrote dag_analytics.run_comparison for batch_id={batch_id}")

    except Exception:
        update_run_status(con, run_id=run_id, status="failed")
        con.commit()
        raise

    finally:
        con.close()
