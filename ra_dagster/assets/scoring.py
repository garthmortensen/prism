from enum import Enum
from pathlib import Path
from typing import Any, Optional

import duckdb
import polars as pl
from dagster import AssetExecutionContext, Config, asset

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


class InvalidGenderOption(str, Enum):
    skip = "skip"
    coerce = "coerce"
    error = "error"


class GenderOption(str, Enum):
    male = "M"
    female = "F"


ModelYearOption = Enum(
    "ModelYearOption",
    {str(y): y for y in range(2021, 2026)},
    type=int,
)


class ScoringConfig(Config):
    model_year: ModelYearOption = ModelYearOption(2024)
    prediction_year: Optional[str] = None
    group_id: Optional[int] = None
    group_description: Optional[str] = None
    run_description: str = "ACA scoring run"
    data_effective: Optional[str] = None
    trigger_source: str = "dagster"
    blueprint_id: Optional[str] = None
    invalid_gender: InvalidGenderOption = InvalidGenderOption.skip
    coerce_gender: Optional[GenderOption] = None


def _read_member_inputs(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    # dbt materializes this as prism.main_intermediate.int_aca_risk_input
    return con.execute(
        """
        SELECT
            member_id,
            date_of_birth,
    ensure_prism_warehouse(con)

    model_year = config.model_year.value
    prediction_year = config.prediction_year
    benefit_year = (
        int(prediction_year) if prediction_year is not None else int(model_year)
    )   """
    ).fetchall()


@asset
def score_members_aca(
    context: AssetExecutionContext, config: ScoringConfig, duckdb: DuckDBResource
) -> None:
    """Score members using the ACA HHS-HCC calculator and write to main_runs.risk_scores."""

    context.log.info(f"Connecting to DuckDB at: {duckdb.path}")
    con = duckdb.get_connection().connect()

    ensure_prism_warehouse(con)

    model_year = config.model_year.value
    prediction_year = config.prediction_year
    benefit_year = int(prediction_year) if prediction_year is not None else int(model_year)

    run_id = context.run_id
    run_ts = generate_run_timestamp()
    git = get_git_provenance(cwd=str(Path(__file__).resolve().parents[2]))

    group_id = config.group_id
    if group_id is None:
        group_id = allocate_group_id(con)

    record = RunRecord(
        run_id=run_id,
        run_timestamp=run_ts,
        group_id=int(group_id),
        group_description=config.group_description,
        run_description=config.run_description,
        analysis_type="scoring",
        calculator="aca_risk_score_calculator",
        model_version=f"hhs_{model_year}",
        benefit_year=benefit_year,
        data_effective=config.data_effective,
        blueprint_yml={
            "model_year": model_year,
            "prediction_year": prediction_year,
            **config.model_dump(),
        },
        git=git,
        status="started",
        trigger_source=config.trigger_source,
        blueprint_id=config.blueprint_id,
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

        invalid_gender = config.invalid_gender.value
        coerce_gender = config.coerce_gender.value if config.coerce_gender else None

        if invalid_gender == "coerce" and coerce_gender is None:
            coerce_gender = "M"

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

        def flush_batch(rows: list[dict[str, Any]]) -> None:
            if not rows:
                return
            df = pl.DataFrame(rows).select(db_columns)
            con.execute("INSERT OR REPLACE INTO main_runs.risk_scores SELECT * FROM df")

        for member in members:
            score = calculator.score(member, prediction_year=prediction_year)

            details = score.details
            components = [comp.model_dump() for comp in score.components]

            out_rows.append(
                {
                    "run_id": run_id,
                    "member_id": str(member.member_id),
                    "risk_score": float(score.risk_score),
                    "hcc_score": float(details.get("hcc_score", 0.0)),
                    "rxc_score": float(details.get("rxc_score", 0.0)),
                    "demographic_score": float(details.get("demographic_factor", 0.0)),
                    "model": details.get("model"),
                    "gender": member.gender,
                    "metal_level": member.metal_level,
                    "enrollment_months": member.enrollment_months,
                    "model_year": model_year,
                    "benefit_year": benefit_year,
                    "calculator": record.calculator,
                    "model_version": record.model_version,
                    "run_timestamp": run_ts,
                    "created_at": created_at,
                    "hcc_list": json_dumps(score.hcc_list),
                    "rxc_list": json_dumps(details.get("rxcs_after_hierarchy", [])),
                    "details": json_dumps(details),
                    "components": json_dumps(components),
                }
            )

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
