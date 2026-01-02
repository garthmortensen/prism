from enum import Enum
from pathlib import Path
from typing import Any
import json
import re

import duckdb
import polars as pl
from dagster import AssetExecutionContext, Config, asset

from ra_calculators.aca_risk_score_calculator import ACACalculator
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


def _relation_exists(con: duckdb.DuckDBPyConnection, relation: str) -> bool:
    parts = relation.split(".")
    if len(parts) == 1:
        (table,) = parts
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = ?
            LIMIT 1
            """,
            [table],
        ).fetchone()
        return row is not None

    if len(parts) == 2:
        schema, table = parts
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        return row is not None

    # For db.schema.table (or similar), try existence via a cheap query.
    try:
        con.execute(f"SELECT 1 FROM {relation} LIMIT 0")
        return True
    except Exception:
        return False


def _resolve_relation(con: duckdb.DuckDBPyConnection, relation: str) -> str:
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
            """
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name = ?
            ORDER BY table_schema
            """,
            [table],
        ).fetchall()
        if len(rows) == 1:
            return f"{rows[0][0]}.{table}"

    raise ValueError(
        f"DuckDB relation not found: {relation!r}. "
        "Check that dbt has been run and that the schema/table name is correct."
    )


def _maybe_build_member_input_view(
    *,
    con: duckdb.DuckDBPyConnection,
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
    con.execute(f"CREATE OR REPLACE TEMP VIEW raw_claims AS SELECT * FROM {claims_view}")
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW raw_enrollments AS SELECT * FROM {enrollments_view}"
    )
    con.execute(f"CREATE OR REPLACE TEMP VIEW raw_members AS SELECT * FROM {members_view}")

    # Mirror dbt models (staging -> intermediate -> int_aca_risk_input), but as TEMP views.
    con.execute(
        """
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
        """
    )

    con.execute(
        """
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
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW stg_enrollment AS
        WITH enrollments AS (
            SELECT * FROM raw_enrollments
        ),
        members AS (
            SELECT * FROM raw_members
        )
        SELECT
            e.member_id,
            CAST(e.start_date AS DATE) AS start_date,
            CAST(e.end_date AS DATE) AS end_date,
            m.gender,
            LOWER(m.plan_metal) AS metal_level,
            CAST(m.dob AS DATE) AS date_of_birth
        FROM enrollments e
        LEFT JOIN members m ON e.member_id = m.member_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW int_member_months AS
        WITH enrollment AS (
            SELECT * FROM stg_enrollment
        )
        SELECT
            member_id,
            LEAST(12, GREATEST(1, DATE_DIFF('month', start_date, end_date) + 1)) AS enrollment_months,
            gender,
            metal_level,
            date_of_birth
        FROM enrollment
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW int_member_diagnoses AS
        WITH diagnoses AS (
            SELECT * FROM stg_claims_dx
        )
        SELECT
            member_id,
            LIST(DISTINCT diagnosis_code) AS diagnosis_list
        FROM diagnoses
        GROUP BY member_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW int_member_rx AS
        WITH rx AS (
            SELECT * FROM stg_claims_rx
        )
        SELECT
            member_id,
            LIST(DISTINCT ndc_code) AS ndc_list
        FROM rx
        GROUP BY member_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW int_aca_risk_input AS
        WITH members AS (
            SELECT * FROM int_member_months
        ),
        diagnoses AS (
            SELECT * FROM int_member_diagnoses
        ),
        rx AS (
            SELECT * FROM int_member_rx
        )
        SELECT
            m.member_id,
            m.enrollment_months,
            m.gender,
            m.metal_level,
            m.date_of_birth,
            COALESCE(d.diagnosis_list, []) AS diagnoses,
            COALESCE(r.ndc_list, []) AS ndc_codes
        FROM members m
        LEFT JOIN diagnoses d ON m.member_id = d.member_id
        LEFT JOIN rx r ON m.member_id = r.member_id
        """
    )

    return "int_aca_risk_input"


@asset
def score_members_aca(
    context: AssetExecutionContext, config: ScoringConfig, duckdb: DuckDBResource
) -> None:
    """Score members using the ACA HHS-HCC calculator and write to main_runs.risk_scores."""

    context.log.info(f"Connecting to DuckDB at: {duckdb.path}")
    con = duckdb.get_connection().connect()

    ensure_prism_warehouse(con)

    diy_model_year = (
        config.model_year.value if config.model_year is not None else config.diy_model_year.value
    )
    member_age_basis_year = config.member_age_basis_year or config.prediction_year
    benefit_year = (
        int(member_age_basis_year)
        if member_age_basis_year is not None
        else int(diy_model_year)
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

    record = RunRecord(
        run_id=run_id,
        run_timestamp=run_ts,
        group_id=int(group_id),
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
            f"""
            SELECT
                member_id,
                date_of_birth,
                gender,
                metal_level,
                enrollment_months,
                diagnoses,
                ndc_codes
            FROM {input_relation}
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
        # This is significantly more I/O intensive than the CSV export which only writes summary
        # scores.
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
            con.register("df_view", df)
            cols_sql = ", ".join(db_columns)
            con.execute(
                f"INSERT OR REPLACE INTO main_runs.risk_scores ({cols_sql}) "
                f"SELECT {cols_sql} FROM df_view"
            )
            con.unregister("df_view")

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
