from __future__ import annotations

from datetime import date
from typing import Any, Iterable

from ra_calculators.aca_risk_score_calculator import MemberInput


def coerce_str_list(value: Any) -> list[str]:
    """Coerce a value (list, tuple, or single item) into a list of strings."""
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


def normalize_gender(value: Any) -> str | None:
    """Normalize gender to 'M' or 'F', or return None if invalid."""
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"M", "MALE"}:
        return "M"
    if text in {"F", "FEMALE"}:
        return "F"
    return None


def rows_to_member_inputs(
    rows: Iterable[tuple[Any, ...]],
    *,
    invalid_gender: str = "skip",
    coerce_gender: str | None = None,
) -> tuple[list[MemberInput], dict[str, Any]]:
    """
    Convert raw database rows into MemberInput objects with validation.
    
    Expected row format:
    (member_id, date_of_birth, gender, metal_level, enrollment_months, diagnoses, ndc_codes)
    """
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

        normalized_gender = normalize_gender(gender)
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
                diagnoses=coerce_str_list(diagnoses),
                ndc_codes=coerce_str_list(ndc_codes),
            )
        )

    return members, {"skipped": skipped, "invalid_gender_values": invalid_gender_values}
