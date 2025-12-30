from __future__ import annotations

import argparse
import csv
import json
import os
import yaml
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from ra_calculators.aca_risk_score_calculator import ACACalculator, MemberInput


def _get_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_duckdb_path() -> str:
    env_path = os.environ.get("DUCKDB_PATH")
    if env_path:
        return env_path
    return str((_get_repo_root() / "risk_adjustment.duckdb").resolve())


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        # Filter out None values and convert to strings
        return [str(x) for x in value if x is not None]
    # DuckDB may return ARRAY as tuple in some cases
    if isinstance(value, tuple):
        # Filter out None values and convert to strings
        return [str(x) for x in value if x is not None]
    return [str(value)]


def _normalize_gender(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"M", "MALE"}:
        return "M"
    if text in {"F", "FEMALE"}:
        return "F"
    return None


def _rows_to_member_inputs(
    rows: Iterable[tuple[Any, ...]],
    *,
    invalid_gender: str,
    coerce_gender: str | None,
) -> tuple[list[MemberInput], dict[str, Any]]:
    members: list[MemberInput] = []
    skipped = 0
    invalid_gender_values: dict[str, int] = {}

    if invalid_gender not in {"skip", "coerce"}:
        raise ValueError("invalid_gender must be one of: skip, coerce")

    if invalid_gender == "coerce":
        if coerce_gender not in {"M", "F"}:
            raise ValueError("coerce_gender must be 'M' or 'F' when invalid_gender='coerce'")

    for (
        member_id,
        date_of_birth,
        gender,
        metal_level,
        enrollment_months,
        diagnoses,
        ndc_codes,
    ) in rows:
        if isinstance(date_of_birth, str):
            date_of_birth = date.fromisoformat(date_of_birth)

        normalized_gender = _normalize_gender(gender)
        if normalized_gender is None:
            raw = "<NULL>" if gender is None else str(gender)
            invalid_gender_values[raw] = invalid_gender_values.get(raw, 0) + 1
            if invalid_gender == "skip":
                skipped += 1
                continue
            normalized_gender = str(coerce_gender)

        members.append(
            MemberInput(
                member_id=str(member_id),
                date_of_birth=date_of_birth,
                gender=normalized_gender,
                metal_level=str(metal_level) if metal_level is not None else "silver",
                enrollment_months=int(enrollment_months) if enrollment_months is not None else 12,
                diagnoses=_coerce_str_list(diagnoses),
                ndc_codes=_coerce_str_list(ndc_codes),
            )
        )

    return members, {"skipped": skipped, "invalid_gender_values": invalid_gender_values}


def score_from_duckdb_to_csv(
    *,
    duckdb_path: str,
    output_csv_path: str,
    model_year: str = "2024",
    prediction_year: str | None = None,
    schema: str = "main_intermediate",
    table: str = "int_aca_risk_input",
    limit: int | None = None,
    invalid_gender: str = "skip",
    coerce_gender: str | None = None,
) -> int:
    """Read member inputs from DuckDB and write ACA risk scores to CSV.

    Returns number of rows written.

    Expected input relation: `{schema}.{table}` with columns:
    - member_id, date_of_birth, gender, metal_level, enrollment_months, diagnoses, ndc_codes
    """

    con = duckdb.connect(str(Path(duckdb_path).expanduser().resolve()))
    try:
        sql = f"""
        SELECT
            member_id,
            date_of_birth,
            gender,
            metal_level,
            enrollment_months,
            diagnoses,
            ndc_codes
        FROM {schema}.{table}
        """.strip()
        if limit is not None:
            sql += f"\nLIMIT {int(limit)}"

        rows = con.execute(sql).fetchall()
        members, stats = _rows_to_member_inputs(
            rows,
            invalid_gender=invalid_gender,
            coerce_gender=coerce_gender,
        )

        calculator = ACACalculator(model_year=model_year)

        output_path = Path(output_csv_path).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "member_id",
            "age",
            "gender",
            "metal_level",
            "enrollment_months",
            "model_year",
            "prediction_year",
            "benefit_year",
            "risk_score",
            "hcc_list",
            "rxc_list",
        ]

        benefit_year = int(prediction_year) if prediction_year is not None else int(model_year)

        # Create directory for YAML exports
        yaml_dir = output_path.parent / "yaml_details"
        yaml_dir.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i, member in enumerate(members):
                score = calculator.score(member, prediction_year=prediction_year)
                
                # Convert components to JSON-serializable format
                components_data = [comp.model_dump() for comp in score.components]
                
                # Extract RXC list from details for backward compatibility with existing queries
                rxc_list = score.details.get("rxcs_after_hierarchy", [])

                if i < 20:
                    yaml_data = {
                        "member_id": member.member_id,
                        "age": score.details.get("age"),
                        "gender": member.gender,
                        "metal_level": member.metal_level,
                        "enrollment_months": member.enrollment_months,
                        "model_year": model_year,
                        "prediction_year": prediction_year,
                        "benefit_year": benefit_year,
                        "risk_score": score.risk_score,
                        "hcc_list": score.hcc_list,
                        "rxc_list": rxc_list,
                        "components": components_data
                    }
                    with (yaml_dir / f"{member.member_id}.yml").open("w", encoding="utf-8") as yf:
                        yaml.dump(yaml_data, yf, sort_keys=False)

                writer.writerow(
                    {
                        "member_id": member.member_id,
                        "age": score.details.get("age"),
                        "gender": member.gender,
                        "metal_level": member.metal_level,
                        "enrollment_months": member.enrollment_months,
                        "model_year": model_year,
                        "prediction_year": prediction_year,
                        "benefit_year": benefit_year,
                        "risk_score": score.risk_score,
                        "hcc_list": json.dumps(score.hcc_list),
                        "rxc_list": json.dumps(rxc_list),
                    }
                )

        skipped = int(stats.get("skipped", 0))
        if skipped:
            total_rows = len(rows)
            pct = (skipped / total_rows) * 100 if total_rows > 0 else 0
            invalids = stats.get("invalid_gender_values", {})
            invalids_str = ", ".join(f"{k}={v}" for k, v in sorted(invalids.items()))
            print(
                f"Skipped {skipped}/{total_rows} ({pct:.2f}%) rows due to invalid gender values: {invalids_str}"
            )

        return len(members)
    finally:
        con.close()


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ra_calculators.aca_risk_score_calculator.duckdb_to_csv",
        description=(
            "Read main_intermediate.int_aca_risk_input from DuckDB and write "
            "ACA risk scores to CSV."
        ),
    )
    p.add_argument(
        "--duckdb-path",
        default=_default_duckdb_path(),
        help="Path to DuckDB file (default: DUCKDB_PATH env var or repo risk_adjustment.duckdb)",
    )
    p.add_argument(
        "--output-csv",
        required=False,
        help="Output CSV path. Defaults to tmp_exports/YYYYMMDD_HHMMSSssss_aca_scores_out.csv",
    )
    p.add_argument(
        "--model-year",
        default="2024",
        help="HHS model year (e.g. 2024)",
    )
    p.add_argument(
        "--prediction-year",
        default=None,
        help="Prediction year; also used as benefit_year if provided",
    )
    p.add_argument(
        "--schema",
        default="main_intermediate",
        help="DuckDB schema containing the input relation",
    )
    p.add_argument(
        "--table",
        default="int_aca_risk_input",
        help="DuckDB table/view name containing the input relation",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for quick smoke tests",
    )

    p.add_argument(
        "--invalid-gender",
        choices=["skip", "coerce"],
        default="skip",
        help="What to do if gender is not M/F: skip row or coerce",
    )
    p.add_argument(
        "--coerce-gender",
        choices=["M", "F"],
        default=None,
        help="When --invalid-gender=coerce, coerce invalid genders to this value",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    output_csv = args.output_csv
    if not output_csv:
        # Format: YYYYMMDD_HHMMSSssss_aca_scores_out.csv
        # Note: %f gives microseconds (6 digits), user asked for ssss (4 digits?) or just milliseconds?
        # Usually ssss implies milliseconds or similar. I'll use microseconds and truncate or just use standard format.
        # User asked for YYYYMMDD_HHMMSSssss. Let's assume they want microseconds or similar unique string.
        # Let's use %f which is microseconds (000000).
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S%f")
        # Write to tmp_exports relative to this script
        output_csv = str(Path(__file__).parent / "tmp_exports" / f"{timestamp}_aca_scores_out.csv")

    count = score_from_duckdb_to_csv(
        duckdb_path=args.duckdb_path,
        output_csv_path=output_csv,
        model_year=str(args.model_year),
        prediction_year=str(args.prediction_year) if args.prediction_year else None,
        schema=str(args.schema),
        table=str(args.table),
        limit=args.limit,
        invalid_gender=str(args.invalid_gender),
        coerce_gender=str(args.coerce_gender) if args.coerce_gender else None,
    )

    print(f"Wrote {count} rows to {Path(output_csv).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
