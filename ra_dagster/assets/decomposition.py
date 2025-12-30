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
def decompose_runs(context, duckdb: DuckDBResource) -> None:
    """Compute a total-level 4-run decomposition into main_analytics.decomposition."""

    config = context.op_config or {}
    run_id_baseline = config.get("run_id_baseline")
    run_id_coeff_only = config.get("run_id_coeff_only")
    run_id_pop_only = config.get("run_id_pop_only")
    run_id_actual = config.get("run_id_actual")

    run_ts_baseline = config.get("run_ts_baseline")
    run_ts_coeff_only = config.get("run_ts_coeff_only")
    run_ts_pop_only = config.get("run_ts_pop_only")
    run_ts_actual = config.get("run_ts_actual")

    if not all([run_id_baseline, run_id_coeff_only, run_id_pop_only, run_id_actual]):
        if not all([run_ts_baseline, run_ts_coeff_only, run_ts_pop_only, run_ts_actual]):
            raise ValueError(
                "decompose_runs requires op config: run_id_* (preferred) or run_ts_* (legacy)"
            )

    analysis_id = context.op_config.get("analysis_id")

    con = duckdb.get_connection().connect()

    ensure_prism_warehouse(con)

    if not all([run_id_baseline, run_id_coeff_only, run_id_pop_only, run_id_actual]):
        def _resolve(ts: str) -> str:
            ids = [
                row[0]
                for row in con.execute(
                    "SELECT DISTINCT run_id FROM main_runs.risk_scores WHERE run_timestamp = ?",
                    [ts],
                ).fetchall()
            ]
            if len(ids) != 1:
                raise ValueError(
                    "run_ts_* must map to exactly one run_id. Pass run_id_* to disambiguate."
                )
            return ids[0]

        run_id_baseline = _resolve(str(run_ts_baseline))
        run_id_coeff_only = _resolve(str(run_ts_coeff_only))
        run_id_pop_only = _resolve(str(run_ts_pop_only))
        run_id_actual = _resolve(str(run_ts_actual))

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
        run_description=config.get("run_description", "4-run decomposition"),
        analysis_type="decomposition",
        calculator=None,
        model_version=None,
        benefit_year=None,
        data_effective=None,
        json_config={
            "run_ts_baseline": run_ts_baseline,
            "run_ts_coeff_only": run_ts_coeff_only,
            "run_ts_pop_only": run_ts_pop_only,
            "run_ts_actual": run_ts_actual,
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
        # Total-level: average risk score per run
        (s00,) = con.execute(
            "SELECT COALESCE(AVG(risk_score), 0.0) FROM main_runs.risk_scores WHERE run_id = ?",
            [run_id_baseline],
        ).fetchone()
        (s01,) = con.execute(
            "SELECT COALESCE(AVG(risk_score), 0.0) FROM main_runs.risk_scores WHERE run_id = ?",
            [run_id_coeff_only],
        ).fetchone()
        (s10,) = con.execute(
            "SELECT COALESCE(AVG(risk_score), 0.0) FROM main_runs.risk_scores WHERE run_id = ?",
            [run_id_pop_only],
        ).fetchone()
        (s11,) = con.execute(
            "SELECT COALESCE(AVG(risk_score), 0.0) FROM main_runs.risk_scores WHERE run_id = ?",
            [run_id_actual],
        ).fetchone()

        total_change = float(s11) - float(s00)
        pop_effect = float(s10) - float(s00)
        coeff_effect = float(s01) - float(s00)
        interaction_effect = total_change - pop_effect - coeff_effect

        con.execute(
            """
            INSERT INTO main_analytics.decomposition (
                analysis_id,
                run_id_baseline,
                run_id_coeff_only,
                run_id_pop_only,
                run_id_actual,
                prior_period,
                current_period,
                prior_model_version,
                current_model_version,
                aggregation_level,
                dimensions,
                total_change,
                pop_effect,
                coeff_effect,
                interaction_effect,
                details,
                created_at
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, CAST(? AS JSON),
                ?, ?, ?, ?,
                CAST(? AS JSON),
                ?
            )
            """,
            [
                analysis_id,
                str(run_id_baseline),
                str(run_id_coeff_only),
                str(run_id_pop_only),
                str(run_id_actual),
                context.op_config.get("prior_period"),
                context.op_config.get("current_period"),
                context.op_config.get("prior_model_version"),
                context.op_config.get("current_model_version"),
                "total",
                json_dumps({}),
                total_change,
                pop_effect,
                coeff_effect,
                interaction_effect,
                json_dumps(
                    {
                        "means": {
                            "baseline": float(s00),
                            "coeff_only": float(s01),
                            "pop_only": float(s10),
                            "actual": float(s11),
                        }
                    }
                ),
                now_utc(),
            ],
        )

        update_run_status(con, run_id=run_id, status="success")
        context.log.info(f"Wrote main_analytics.decomposition for group_id={group_id}")

    except Exception:
        update_run_status(con, run_id=run_id, status="failed")
        raise

    finally:
        con.close()
