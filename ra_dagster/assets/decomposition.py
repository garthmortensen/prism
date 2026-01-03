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
)


@asset
def decompose_runs(context, duckdb: DuckDBResource) -> None:
    """
    Compute an N-way decomposition of risk score changes using member-level deltas.

    Decomposes the difference between a baseline run and an actual run into
    specific component effects defined by intermediate runs.

    Supports one methodology:
    1. "marginal": Calculates each component's effect independently against the baseline.
       Interaction is the residual difference.

    Config:
        run_id_baseline: str
        run_id_actual: str
        population_mode: str = "intersection" | "baseline_population" | "scenario_population"
            (default: "intersection")
        components: List[Dict]
            name: str
            run_id: str
            description: str (optional)
            population_mode: str (optional override)
    """

    config = context.op_config or {}
    con = duckdb.get_connection().connect()

    try:
        ensure_prism_warehouse(con)

        # Handle configuration
        run_id_baseline = config.get("baseline_run_id")
        run_id_actual = config.get("actual_run_id")

        if not run_id_baseline:
            raise ValueError("decompose_runs config requires 'baseline_run_id'.")
        if not run_id_actual:
            raise ValueError("decompose_runs config requires 'actual_run_id'.")

        # Hardcoded constants
        method = "marginal"
        metric = "mean"
        global_pop_mode = config.get("population_mode", "intersection")

        if global_pop_mode != "intersection":
            raise ValueError(
                "Global population_mode must be 'intersection' for marginal decomposition. "
                f"Got '{global_pop_mode}'."
            )

        components = config.get("components", [])
        for comp in components:
            if "run_id" not in comp:
                raise ValueError(f"Component '{comp.get('name', 'unnamed')}' requires a 'run_id'.")
            if "name" not in comp:
                raise ValueError("Component requires a 'name'.")

        # 3. Fetch Metadata from Actual Run for RunRecord
        meta_row = con.execute(
            """
            SELECT model_version, benefit_year
            FROM main_runs.run_registry
            WHERE run_id = ?
            """,
            [run_id_actual],
        ).fetchone()

        actual_model_version = meta_row[0] if meta_row else None
        actual_benefit_year = meta_row[1] if meta_row else None

        # 4. Create Run Record
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
            run_description=config.get("run_description", f"N-way decomposition ({method})"),
            analysis_type="decomposition",
            calculator=None,
            model_version=actual_model_version,
            benefit_year=actual_benefit_year,
            launchpad_config=extract_launchpad_config(context=context, fallback=config),
            blueprint_yml={
                "run_id_baseline": run_id_baseline,
                "run_id_actual": run_id_actual,
                "method": method,
                "metric": metric,
                "population_mode": global_pop_mode,
                "components": components,
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

        # 5. Calculate Effects
        batch_id = context.run_id

        def calculate_impact(run_a, run_b, mode):
            # mode: intersection, baseline_population, scenario_population

            agg_func = "AVG"

            cte_sql = """
                WITH A AS (SELECT member_id, risk_score FROM main_runs.risk_scores
                           WHERE run_id = ?),
                     B AS (SELECT member_id, risk_score FROM main_runs.risk_scores
                           WHERE run_id = ?)
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

            res = con.execute(query, [run_a, run_b]).fetchone()
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
        con.executemany(
            "INSERT INTO main_analytics.decomposition_definitions "
            "(batch_id, step_index, driver_name, description, created_at) VALUES (?, ?, ?, ?, ?)",
            [(*d, now_utc()) for d in definitions],
        )

        con.executemany(
            "INSERT INTO main_analytics.decomposition_scenarios "
            "(batch_id, driver_name, impact_value, run_id, created_at) VALUES (?, ?, ?, ?, ?)",
            [(*s, now_utc()) for s in scenarios],
        )

        update_run_status(con, run_id=run_id, status="success")
        context.log.info(f"Wrote decomposition definitions and scenarios for batch_id={batch_id}")

    except Exception:
        # If run_id was created, mark it failed
        if "run_id" in locals():
            update_run_status(con, run_id=run_id, status="failed")
        raise

    finally:
        con.close()
