"""
Asset: decomposition.py
Description:
    Performs N-way decomposition of risk score changes.
    - Isolates the impact of specific factors (e.g., Model Version, Population Mix).
    - Calculates marginal effects and interaction residuals.
    - Writes scenario results to `main_analytics`.

Usage:
    Executed via the `decomposition_job` in Dagster.
"""

import getpass
from pathlib import Path

from dagster import asset, ResourceParam, AssetExecutionContext

from ra_dagster.db.bootstrap import ensure_prism_warehouse, now_utc
from ra_dagster.db.run_registry import (
    RunRecord,
    allocate_group_id,
    allocate_run_seq,
    insert_run,
    update_run_status,
    resolve_run_id,
)
from ra_dagster.utils.run_refs import generate_run_ref
from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource
from ra_dagster.utils.run_ids import (
    extract_launchpad_config,
    generate_run_timestamp,
    get_git_provenance,
)
from sqlalchemy import text

from ra_dagster.config_schemas import DecompositionConfig


@asset
def decompose_runs(context: AssetExecutionContext, config: DecompositionConfig, database: ResourceParam[SqlAlchemyResource]) -> None:
    """
    Compute an N-way decomposition of risk score changes using member-level deltas.

    Decomposes the difference between a baseline run and an actual run into
    specific component effects defined by intermediate runs.

    Supports one methodology:
    1. "marginal": Calculates each component's effect independently against the baseline.
       Interaction is the residual difference.

    Config:
        baseline_run_ref: str
        actual_run_ref: str
        population_mode: str = "intersection" | "baseline_population" | "scenario_population"
            (default: "intersection")
        components: List[Dict]
            name: str
            run_ref: str
            description: str (optional)
            population_mode: str (optional override)
    """

    engine = database.get_engine()
    con = engine.connect()

    try:
        ensure_prism_warehouse(con)

        # Handle configuration
        run_ref_baseline_raw = config.baseline_run_ref
        run_ref_actual_raw = config.actual_run_ref

        # Resolve runs
        run_id_baseline = resolve_run_id(con, run_ref_baseline_raw)
        run_id_actual = resolve_run_id(con, run_ref_actual_raw)

        # Hardcoded constants
        method = "marginal"
        metric = "mean"
        global_pop_mode = config.population_mode

        if global_pop_mode != "intersection":
            raise ValueError(
                "Global population_mode must be 'intersection' for marginal decomposition. "
                f"Got '{global_pop_mode}'."
            )

        # Process components
        components = []
        for comp_config in config.components:
            comp_data = {
                "name": comp_config.name,
                "run_ref": comp_config.run_ref,
                "description": comp_config.description,
                "population_mode": comp_config.population_mode,
                "run_id": resolve_run_id(con, comp_config.run_ref),
            }
            components.append(comp_data)

        # 3. Fetch Metadata from Actual Run for RunRecord
        meta_row = con.execute(
            text("""
            SELECT model_version, benefit_year
            FROM main_runs.run_registry
            WHERE run_id = :run_id
            """),
            {"run_id": run_id_actual},
        ).fetchone()

        actual_model_version = meta_row[0] if meta_row else None
        actual_benefit_year = meta_row[1] if meta_row else None

        # 4. Create Run Record
        run_id = context.run_id
        run_ts = generate_run_timestamp()
        git = get_git_provenance(cwd=str(Path(__file__).resolve().parents[2]))

        group_id = config.group_id
        if group_id is None:
            group_id = allocate_group_id(con)

        run_seq = allocate_run_seq(con)
        run_ref = generate_run_ref(run_seq, width=4, prefix="d")
        group_ref = (
            generate_run_ref(int(group_id), width=4, prefix="b")
            if group_id is not None
            else None
        )

        context.log.info(f"Run Ref: {run_ref}")
        if group_ref:
            context.log.info(f"Batch Ref: {group_ref}")

        # Convert config to dict for safe usage in blueprint and fallback
        # dagster.Config methods usually have a way to dump, but explicitly unpacking is safest for now
        # avoiding .dict() call if version mismatch issues arise, manually extracting known fields if needed
        # but let's try .dict() or standard python vars() trick isn't good for pydantic. 
        # Actually, let's just make a dict of it.
        config_as_dict = {
            "baseline_run_ref": config.baseline_run_ref,
            "actual_run_ref": config.actual_run_ref,
            "population_mode": config.population_mode,
            "components": [c.dict() for c in config.components], # .dict() usually exists on Config/Pydantic
            "group_id": config.group_id,
            "group_description": config.group_description,
            "run_description": config.run_description,
            "trigger_source": config.trigger_source,
            "blueprint_id": config.blueprint_id,
        }

        record = RunRecord(
            run_id=run_id,
            run_seq=run_seq,
            run_ref=run_ref,
            run_timestamp=run_ts,
            group_id=int(group_id),
            group_ref=group_ref,
            group_description=config.group_description,
            run_description=config.run_description,
            analysis_type="decomposition",
            calculator=None,
            model_version=actual_model_version,
            benefit_year=actual_benefit_year,
            launchpad_config=extract_launchpad_config(context=context, fallback=config_as_dict),
            blueprint_yml={
                "run_id_baseline": run_id_baseline,
                "run_id_actual": run_id_actual,
                "method": method,
                "metric": metric,
                "population_mode": global_pop_mode,
                "components": components,
                **config_as_dict,
            },
            git=git,
            status="started",
            trigger_source=config.trigger_source,
            blueprint_id=str(config.blueprint_id)
            if config.blueprint_id is not None
            else None,
            whoami=getpass.getuser(),
            created_at=now_utc(),
            updated_at=now_utc(),
        )

        insert_run(con, record)

        # 5. Calculate Effects
        batch_id = context.run_id

        def calculate_impact(run_a, run_b, mode):
            """Calculate the impact of a specific driver on the risk score."""
            # mode: intersection, baseline_population, scenario_population

            agg_func = "AVG"

            cte_sql = """
                WITH A AS (SELECT member_id, risk_score FROM main_runs.risk_scores
                           WHERE run_id = :run_a),
                     B AS (SELECT member_id, risk_score FROM main_runs.risk_scores
                           WHERE run_id = :run_b)
            """

            if mode == "intersection":
                query = f"""
                    {cte_sql}
                    SELECT {agg_func}(b.risk_score - a.risk_score)
                    FROM A
                    INNER JOIN B ON A.member_id = B.member_id
                """
            elif mode == "baseline_population":
                query = f"""
                    {cte_sql}
                    SELECT {agg_func}(COALESCE(b.risk_score, 0.0) - a.risk_score)
                    FROM A
                    LEFT JOIN B ON A.member_id = B.member_id
                """
            elif mode == "scenario_population":
                # Use LEFT JOIN starting from B instead of RIGHT JOIN
                query = f"""
                    {cte_sql}
                    SELECT {agg_func}(b.risk_score - COALESCE(a.risk_score, 0.0))
                    FROM B
                    LEFT JOIN A ON B.member_id = A.member_id
                """
            else:
                raise ValueError(f"Unknown population_mode: {mode}")

            res = con.execute(text(query), {"run_a": run_a, "run_b": run_b}).fetchone()
            return float(res[0]) if res and res[0] is not None else 0.0

        # Calculate Total Change
        total_change = calculate_impact(run_id_baseline, run_id_actual, global_pop_mode)

        definitions = []
        scenarios = []

        sum_effects = 0.0

        for i, comp in enumerate(components):
            rid = comp["run_id"]
            pop_mode = comp.get("population_mode", global_pop_mode)

            # marginal
            effect = calculate_impact(run_id_baseline, rid, pop_mode)

            sum_effects += effect

            definitions.append(
                (
                    batch_id,
                    i + 1,
                    comp["name"],
                    comp.get("description", f"Impact of {comp['name']}"),
                )
            )
            scenarios.append((batch_id, comp["name"], effect, str(rid)))

        # Interaction (Residual)
        # In marginal, residual is total change minus sum of partial effects
        interaction_effect = total_change - sum_effects

        definitions.append(
            (
                batch_id,
                len(components) + 1,
                "Interaction",
                "Combined interaction effect of all factors",
            )
        )
        scenarios.append(
            (
                batch_id,
                "Interaction",
                float(interaction_effect),
                str(run_id_actual),
            )
        )
        
        con.execute(
            text("""
            INSERT INTO main_analytics.decomposition_definitions 
            (batch_id, step_index, driver_name, description, created_at) 
            VALUES (:batch_id, :step_index, :driver_name, :description, :created_at)
            """),
            [
                {
                    "batch_id": d[0],
                    "step_index": d[1],
                    "driver_name": d[2],
                    "description": d[3],
                    "created_at": now_utc()
                }
                for d in definitions
            ],
        )

        con.execute(
            text("""
            INSERT INTO main_analytics.decomposition_scenarios 
            (batch_id, driver_name, impact_value, run_id, created_at) 
            VALUES (:batch_id, :driver_name, :impact_value, :run_id, :created_at)
            """),
            [
                {
                    "batch_id": s[0],
                    "driver_name": s[1],
                    "impact_value": s[2],
                    "run_id": s[3],
                    "created_at": now_utc()
                }
                for s in scenarios
            ],
        )

        update_run_status(con, run_id=run_id, status="success")
        con.commit()
        context.log.info(f"Wrote decomposition definitions and scenarios for batch_id={batch_id}")

    except Exception:
        # If run_id was created, mark it failed
        if "run_id" in locals():
            update_run_status(con, run_id=run_id, status="failed")
        raise

    finally:
        con.close()
