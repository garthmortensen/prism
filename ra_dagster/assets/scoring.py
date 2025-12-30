from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import polars as pl
from dagster import AssetExecutionContext, asset

from ra_calculators.aca_risk_score_calculator import ACACalculator, MemberInput
from ra_calculators.aca_risk_score_calculator.member_processing import rows_to_member_inputs
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


def _read_member_inputs(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    # dbt materializes this as prism.main_intermediate.int_aca_risk_input
    return con.execute(
        """
        SELECT
            member_id,
            date_of_birth,
            gender,
            metal_level,
            enrollment_months,
            diagnoses,
            ndc_codes
        FROM main_intermediate.int_aca_risk_input
        """
    ).fetchall()


@asset
def score_members_aca(context, duckdb: DuckDBResource) -> None:
    """Score members using the ACA HHS-HCC calculator and write to main_runs.risk_scores."""

    context.log.info(f"Connecting to DuckDB at: {duckdb.path}")
    con = duckdb.get_connection().connect()

    ensure_prism_warehouse(con)

    config = context.op_config or {}
    model_year = str(config.get("model_year", "2024"))
    prediction_year = config.get("prediction_year")
    benefit_year = int(prediction_year) if prediction_year is not None else int(model_year)

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
        run_description=config.get("run_description", "ACA scoring run"),
        analysis_type="scoring",
        calculator="aca_risk_score_calculator",
        model_version=f"hhs_{model_year}",
        benefit_year=benefit_year,
        data_effective=config.get("data_effective"),
        json_config={
            "model_year": model_year,
            "prediction_year": prediction_year,
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
        calculator = ACACalculator(model_year=model_year)

        rows = con.execute(
            """
            SELECT
                member_id,
                date_of_birth,
                gender,
                metal_level,
                enrollment_months,
                diagnoses,
                ndc_codes
            FROM main_intermediate.int_aca_risk_input
            """
        ).fetchall()

        invalid_gender = config.get("invalid_gender", "skip")
        coerce_gender = config.get("coerce_gender")

        members, stats = rows_to_member_inputs(
            rows,
            invalid_gender=invalid_gender,
            coerce_gender=coerce_gender,
        )

        if stats["skipped"] > 0:
            context.log.warning(f"Skipped {stats['skipped']} members due to invalid data.")
        if stats["invalid_gender_values"]:
            context.log.info(f"Invalid gender values encountered: {stats['invalid_gender_values']}")

        context.log.info(f"Starting scoring for {len(members)} members...")

        # Performance Note:
        # This asset writes full calculation details (JSON) and component breakdowns to the DB.
        # This is significantly more I/O intensive than the CSV export which only writes summary scores.
        # We use Polars for bulk insertion to minimize overhead.

        batch_size = 10000
        out_rows: list[dict[str, Any]] = []
        created_at = now_utc()
        total_written = 0

        # Columns must match main_runs.risk_scores definition order
        db_columns = [
            "run_id",
            "run_timestamp",
            "member_id",
            "calculator",
            "model_version",
            "model_year",
            "benefit_year",
            "risk_score",
            "demographic_score",
            "hcc_score",
            "rxc_score",
            "hcc_list",
            "rxc_list",
            "details",
            "components",
            "created_at",
        ]

        def flush_batch(rows: list[dict[str, Any]]) -> None:
            if not rows:
                return
            df = pl.DataFrame(rows).select(db_columns)
            con.execute("INSERT OR REPLACE INTO main_runs.risk_scores SELECT * FROM df")

        for member in members:
            score = calculator.score(member, prediction_year=prediction_year)

            details = score.details
            components = [comp.model_dump() for comp in score.components]
            
            out_rows.append({
                "run_id": run_id,
                "run_timestamp": run_ts,
                "member_id": str(member.member_id),
                "calculator": record.calculator,
                "model_version": record.model_version,
                "model_year": model_year,
                "benefit_year": benefit_year,
                "risk_score": float(score.risk_score),
                "demographic_score": float(details.get("demographic_factor", 0.0)),
                "hcc_score": float(details.get("hcc_score", 0.0)),
                "rxc_score": float(details.get("rxc_score", 0.0)),
                "hcc_list": json_dumps(score.hcc_list),
                "rxc_list": json_dumps(details.get("rxcs_after_hierarchy", [])),
                "details": json_dumps(details),
                "components": json_dumps(components),
                "created_at": created_at,
            })

            if len(out_rows) >= batch_size:
                flush_batch(out_rows)
                total_written += len(out_rows)
                out_rows = []
                context.log.info(f"Scored and wrote {total_written}/{len(members)} members")

        flush_batch(out_rows)
        total_written += len(out_rows)

        update_run_status(con, run_id=run_id, status="success")
        context.log.info(
            f"Wrote {total_written} rows to main_runs.risk_scores for run_timestamp={run_ts}"
        )

    except Exception:
        update_run_status(con, run_id=run_id, status="failed")
        raise

    finally:
        con.close()
