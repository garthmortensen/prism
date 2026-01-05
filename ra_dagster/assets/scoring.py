"""
Asset: scoring.py
Description:
    Core risk adjustment calculation logic.
    - Resolves input views (claims, enrollment, members).
    - Normalizes data into a standard input format.
    - Executes the HHS-HCC risk model (via `ra_calculators`).
    - Writes detailed results to `main_runs.risk_scores`.

Usage:
    Executed via the `scoring_job` in Dagster.
"""
import json
import re
from enum import Enum
from pathlib import Path
from typing import Any

import polars as pl
from dagster import AssetExecutionContext, Config, asset, ResourceParam
from sqlalchemy import text
from sqlalchemy.engine import Connection

from ra_calculators.aca_risk_score_calculator import ACACalculator
from ra_calculators.aca_risk_score_calculator.member_processing import rows_to_member_inputs
from ra_dagster.db.bootstrap import ensure_prism_warehouse, now_utc
from ra_dagster.db.run_registry import (
    RunRecord,
    allocate_group_id,
    allocate_run_seq,
    insert_run,
    update_run_status,
)
from ra_dagster.utils.human_ids import generate_human_id
from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource
from ra_dagster.utils.run_ids import (
    extract_launchpad_config,
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
    # DIY tables year (controls coefficients/mappings/hierarchies/etc.).
    diy_model_year: ModelYearOption = ModelYearOption(2024)
    # Backwards-compatible alias for diy_model_year.
    model_year: ModelYearOption | None = None
    # Year used for DOB-based age calculation (age as-of 12/31 of this year).
    # Preferred name; replaces prediction_year.
    member_age_basis_year: str | None = None
    # Legacy alias for member_age_basis_year.
    prediction_year: str | None = None
    group_id: int | None = None
    group_description: str | None = None
    run_description: str = "ACA scoring run"
    trigger_source: str = "dagster"
    blueprint_id: str | None = None
    invalid_gender: InvalidGenderOption = InvalidGenderOption.skip
    coerce_gender: GenderOption | None = None

    # Optional: parameterize which raw views feed scoring inputs.
    # If any are set, all three must be set.
    claims_view: str | None = None
    enrollments_view: str | None = None
    members_view: str | None = None


_RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


def _validate_relation_name(relation: str) -> str:
    """Allow only simple identifiers like schema.table (or db.schema.table)."""
    if not _RELATION_RE.fullmatch(relation):
        raise ValueError(
            "Invalid relation name. Expected like 'schema.table' (letters/numbers/_ only). "
            f"Got: {relation!r}"
        )
    return relation


def _relation_exists(con: Connection, relation: str) -> bool:
    """Check if a relation (table or view) exists in the database."""
    parts = relation.split(".")
    if len(parts) == 1:
        (table,) = parts
        row = con.execute(
            text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = :table
            LIMIT 1
            """),
            {"table": table},
        ).fetchone()
        return row is not None

    if len(parts) == 2:
        schema, table = parts
        row = con.execute(
            text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :table
            LIMIT 1
            """),
            {"schema": schema, "table": table},
        ).fetchone()
        return row is not None

    # For db.schema.table (or similar), try existence via a cheap query.
    try:
        con.execute(text(f"SELECT 1 FROM {relation} LIMIT 0"))
        return True
    except Exception:
        return False


def _resolve_relation(con: Connection, relation: str) -> str:
    """Resolve a relation name against DuckDB, handling common dbt/DuckDB prefixing.

    DuckDB + dbt sometimes materialize a configured schema like `main_raw` as
    `main_main_raw` (database+schema concatenation). If the user provides
    `main_raw.table`, we try that first, then fall back to `main_main_raw.table`.
    """
    relation = _validate_relation_name(relation)

    if _relation_exists(con, relation):
        return relation

    parts = relation.split(".")
    if len(parts) == 2:
        schema, table = parts

        # Common fallback: dbt may materialize `main_raw` as `main_main_raw`.
        alt = f"main_{schema}.{table}"
        if _relation_exists(con, alt):
            return alt

        # Common fallback: prefix schema with "main_".
        if not schema.startswith("main_"):
            alt = f"main_{schema}.{table}"
            if _relation_exists(con, alt):
                return alt

        # Also try stripping a leading "main_" if present.
        if schema.startswith("main_"):
            alt = f"{schema.removeprefix('main_')}.{table}"
            if _relation_exists(con, alt):
                return alt

        # As a last resort, try to locate it by table name.
        rows = con.execute(
            text("""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name = :table
            ORDER BY table_schema
            """),
            {"table": table},
        ).fetchall()
        if len(rows) == 1:
            return f"{rows[0][0]}.{table}"

    raise ValueError(
        f"Database relation not found: {relation!r}. "
        "Check that dbt has been run and that the schema/table name is correct."
    )


def _maybe_build_member_input_view(
    *,
    con: Connection,
    claims_view: str | None,
    enrollments_view: str | None,
    members_view: str | None,
) -> str:
    """Return the relation to read member inputs from.

    - Default: use dbt-produced `main_intermediate.int_aca_risk_input`.
    - If views are provided: build a TEMP view `int_aca_risk_input` from those sources.
    """

    any_set = any(v is not None for v in (claims_view, enrollments_view, members_view))
    if not any_set:
        return "main_intermediate.int_aca_risk_input"

    if not all(v is not None for v in (claims_view, enrollments_view, members_view)):
        raise ValueError(
            "If overriding sources, you must set claims_view, enrollments_view, and members_view."
        )

    claims_view = _resolve_relation(con, claims_view)
    enrollments_view = _resolve_relation(con, enrollments_view)
    members_view = _resolve_relation(con, members_view)

    # Create temp aliases matching dbt seed names so the downstream SQL is identical.
    con.execute(text(f"CREATE OR REPLACE TEMP VIEW raw_claims AS SELECT * FROM {claims_view}"))
    con.execute(text(f"CREATE OR REPLACE TEMP VIEW raw_enrollments AS SELECT * FROM {enrollments_view}"))
    con.execute(text(f"CREATE OR REPLACE TEMP VIEW raw_members AS SELECT * FROM {members_view}"))

    # Mirror dbt models (staging -> intermediate -> int_aca_risk_input), but as TEMP views.
    con.execute(
        text("""
        CREATE OR REPLACE TEMP VIEW stg_claims_dx AS
        WITH source AS (
            SELECT * FROM raw_claims
        )
        SELECT
            claim_id,
            member_id,
            CAST(service_date AS DATE) AS service_date,
            REPLACE(diagnosis_code, '.', '') AS diagnosis_code
        FROM source
        WHERE diagnosis_code IS NOT NULL
          AND claim_type != 'RX'
        """)
    )

    con.execute(
        text("""
        CREATE OR REPLACE TEMP VIEW stg_claims_rx AS
        WITH source AS (
            SELECT * FROM raw_claims
        )
        SELECT
            claim_id,
            member_id,
            CAST(service_date AS DATE) AS fill_date,
            drug AS ndc_code
        FROM source
        WHERE claim_type = 'RX'
        """)
    )

    con.execute(
        text("""
        CREATE OR REPLACE TEMP VIEW stg_enrollment AS
        WITH enrollments AS (
            SELECT * FROM raw_enrollments
        ),
        members AS (
            SELECT * FROM raw_members
        )
        SELECT
            e.member_id,
            m.gender,
            m.dob,
            e.start_date,
            e.end_date,
            m.plan_metal AS metal_level,
            m.enrollment_length_continuous AS enrollment_months
        FROM enrollments e
        LEFT JOIN members m ON e.member_id = m.member_id
        """)
    )

    con.execute(
        text("""
        CREATE OR REPLACE TEMP VIEW int_aca_risk_input AS
        WITH enrollment AS (
            SELECT * FROM stg_enrollment
        ),
        claims_dx AS (
            SELECT * FROM stg_claims_dx
        ),
        claims_rx AS (
            SELECT * FROM stg_claims_rx
        )
        SELECT
            e.member_id,
            e.gender,
            e.dob,
            e.start_date,
            e.end_date,
            e.metal_level,
            e.enrollment_months,
            dx.diagnosis_code AS diagnoses,
            dx.service_date AS diagnosis_service_date,
            rx.ndc_code AS ndc_codes,
            rx.fill_date AS rx_fill_date
        FROM enrollment e
        LEFT JOIN claims_dx dx ON e.member_id = dx.member_id
        LEFT JOIN claims_rx rx ON e.member_id = rx.member_id
        """)
    )

    return "int_aca_risk_input"





@asset
def score_members_aca(
    context: AssetExecutionContext, config: ScoringConfig, database: ResourceParam[SqlAlchemyResource]
) -> None:
    """Score members using the ACA HHS-HCC calculator and write to main_runs.risk_scores."""

    engine = database.get_engine()
    context.log.info(f"Connecting to database: {engine.url}")
    con = engine.connect()

    ensure_prism_warehouse(con)

    diy_model_year = (
        config.model_year.value if config.model_year is not None else config.diy_model_year.value
    )
    member_age_basis_year = config.member_age_basis_year or config.prediction_year
    benefit_year = (
        int(member_age_basis_year) if member_age_basis_year is not None else int(diy_model_year)
    )

    resolved_claims_view = (
        _resolve_relation(con, config.claims_view) if config.claims_view is not None else None
    )
    resolved_enrollments_view = (
        _resolve_relation(con, config.enrollments_view)
        if config.enrollments_view is not None
        else None
    )
    resolved_members_view = (
        _resolve_relation(con, config.members_view) if config.members_view is not None else None
    )

    context.log.info(
        "Effective scoring config (including resolved sources):\n"
        + json.dumps(
            {
                "config": config.model_dump(),
                "effective": {
                    "diy_model_year": int(diy_model_year),
                    "member_age_basis_year": int(member_age_basis_year)
                    if member_age_basis_year is not None
                    else None,
                    "benefit_year": int(benefit_year),
                },
                "resolved_sources": {
                    "claims_view": resolved_claims_view,
                    "enrollments_view": resolved_enrollments_view,
                    "members_view": resolved_members_view,
                },
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )

    input_relation = _maybe_build_member_input_view(
        con=con,
        claims_view=resolved_claims_view,
        enrollments_view=resolved_enrollments_view,
        members_view=resolved_members_view,
    )

    run_id = context.run_id
    run_ts = generate_run_timestamp()
    git = get_git_provenance(cwd=str(Path(__file__).resolve().parents[2]))

    group_id = config.group_id
    if group_id is None:
        group_id = allocate_group_id(con)

    run_seq = allocate_run_seq(con)
    run_code = generate_human_id(run_seq, width=4, prefix="s")
    group_code = (
        generate_human_id(int(group_id), width=4, prefix="b")
        if group_id is not None
        else None
    )

    record = RunRecord(
        run_id=run_id,
        run_seq=run_seq,
        run_code=run_code,
        run_timestamp=run_ts,
        group_id=int(group_id),
        group_code=group_code,
        group_description=config.group_description,
        run_description=config.run_description,
        analysis_type="scoring",
        calculator="aca_risk_score_calculator",
        model_version=f"hhs_{diy_model_year}",
        benefit_year=benefit_year,
        launchpad_config=extract_launchpad_config(
            context=context,
            fallback={
                "ops": {
                    "score_members_aca": {
                        "config": config.model_dump(),
                    }
                }
            },
        ),
        blueprint_yml={
            "diy_model_year": diy_model_year,
            "member_age_basis_year": member_age_basis_year,
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
        calculator = ACACalculator(model_year=str(diy_model_year))

        rows = con.execute(
            text(f"""
            SELECT
                member_id,
                ANY_VALUE(dob) AS date_of_birth,
                ANY_VALUE(gender) AS gender,
                ANY_VALUE(metal_level) AS metal_level,
                ANY_VALUE(enrollment_months) AS enrollment_months,
                LIST(DISTINCT diagnoses) AS diagnoses,
                LIST(DISTINCT ndc_codes) AS ndc_codes
            FROM {input_relation}
            GROUP BY member_id
            ORDER BY member_id
            """)
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
        # This is significantly more I/O intensive than the CSV export which only writes summary
        # scores.
        # We use Polars for bulk insertion to minimize overhead.

        # Clean up any existing data for this run_id (e.g. from a retry)
        con.execute(
            text("DELETE FROM main_runs.risk_scores WHERE run_id = :run_id"),
            {"run_id": run_id}
        )

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
            """Flush a batch of scored members to the database."""
            if not rows:
                return
            df = pl.DataFrame(rows).select(db_columns)
            df.to_pandas().to_sql(
                "risk_scores",
                con=con,
                schema="main_runs",
                if_exists="append",
                index=False,
            )

        for member in members:
            score = calculator.score(
                member,
                prediction_year=int(member_age_basis_year)
                if member_age_basis_year is not None
                else None,
            )

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
                    "model_year": int(diy_model_year),
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
