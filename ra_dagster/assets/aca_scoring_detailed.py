"""Dagster assets for granular ACA HHS-HCC risk scoring pipeline.

This module provides a detailed DAG view of the risk scoring process,
breaking down the monolithic `score_members_aca` into discrete steps:

Asset Dependency Graph:
    aca_member_input
           │
           ▼
    aca_demographics ──────────────────────────────────┐
           │                                           │
           ▼                                           │
    aca_raw_ccs ◄── aca_icd_to_cc_mapping              │
           │                                           │
           ▼                                           │
    aca_hccs_post_hierarchy ◄── aca_hcc_hierarchies    │
           │                                           │
           ▼                                           │
    aca_hccs_final ◄── aca_model_exclusions            │
           │           aca_hcc_groups_*                │
           │                                           │
           ▼                                           │
    aca_rxcs ◄── aca_ndc_to_rxc_mapping                │
           │     aca_rxc_hierarchies                   │
           │                                           │
           ▼                                           ▼
    aca_scores ◄────────────────────────── aca_coefficients

Each asset produces a DataFrame that can be inspected in the Dagster UI,
enabling debugging and understanding of each scoring step.
"""

from datetime import date
from typing import Any

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
    ResourceParam,
    asset,
)
from sqlalchemy import text

from ra_calculators.aca_risk_score_calculator import (
    apply_hcc_groupings,
    apply_hcc_hierarchies,
    apply_rxc_hierarchies,
    calculate_age,
    calculate_edf,
    calculate_hcc_scores,
    calculate_rxc_scores,
    calculate_total_score,
    exclude_model_hccs,
    get_coefficient,
    get_demographic_variable,
    get_model_type,
    map_diagnoses_to_ccs,
    map_ndcs_to_rxcs,
)
from ra_dagster.config_schemas import ScoringConfig
from ra_dagster.db.bootstrap import ensure_prism_warehouse
from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource


# =============================================================================
# Step 1: Member Input
# =============================================================================


@asset(
    group_name="aca_scoring_detailed",
    description="Raw member input data from the database. Contains member demographics, "
    "diagnoses, and NDC codes aggregated by member_id.",
)
def aca_member_input(
    context: AssetExecutionContext,
    config: ScoringConfig,
    database: ResourceParam[SqlAlchemyResource],
) -> pl.DataFrame:
    """Load member input data from the database."""
    engine = database.get_engine()
    con = engine.connect()
    ensure_prism_warehouse(con)

    # Use dbt-produced view by default
    input_relation = "intermediate.int_aca_risk_input"

    context.log.info(f"Loading member input from {input_relation}")

    rows = con.execute(
        text(f"""
        SELECT
            member_id,
            MAX(dob) AS date_of_birth,
            MAX(gender) AS gender,
            MAX(metal_level) AS metal_level,
            MAX(enrollment_months) AS enrollment_months,
            ARRAY_AGG(DISTINCT diagnoses) FILTER (WHERE diagnoses IS NOT NULL) AS diagnoses,
            ARRAY_AGG(DISTINCT ndc_codes) FILTER (WHERE ndc_codes IS NOT NULL) AS ndc_codes
        FROM {input_relation}
        GROUP BY member_id
        ORDER BY member_id
        """)
    ).fetchall()

    con.close()

    # Convert to Polars DataFrame
    data = []
    for row in rows:
        data.append({
            "member_id": str(row[0]),
            "date_of_birth": row[1],
            "gender": row[2],
            "metal_level": row[3] or "silver",
            "enrollment_months": row[4] or 12,
            "diagnoses": list(row[5]) if row[5] else [],
            "ndc_codes": list(row[6]) if row[6] else [],
        })

    df = pl.DataFrame(data)

    context.log.info(f"Loaded {len(df)} members")
    context.add_output_metadata({
        "member_count": len(df),
        "columns": df.columns,
    })

    return df


# =============================================================================
# Step 2: Demographics
# =============================================================================


@asset(
    group_name="aca_scoring_detailed",
    description="Calculates age, model type (Adult/Child/Infant), and demographic "
    "variable/coefficient for each member.",
    ins={"aca_member_input": AssetIn()},
)
def aca_demographics(
    context: AssetExecutionContext,
    config: ScoringConfig,
    aca_member_input: pl.DataFrame,
    aca_coefficients: dict[tuple[str, str], dict[str, float]],
) -> pl.DataFrame:
    """Calculate demographics for each member."""
    model_year = config.diy_model_year.value
    member_age_basis_year = config.member_age_basis_year or str(model_year)
    benefit_year_end = date(int(member_age_basis_year), 12, 31)

    context.log.info(f"Calculating demographics as of {benefit_year_end}")

    results = []
    for row in aca_member_input.iter_rows(named=True):
        dob = row["date_of_birth"]
        if isinstance(dob, str):
            dob = date.fromisoformat(dob)

        gender = row["gender"]
        metal_level = row["metal_level"]

        # Skip invalid records
        if gender not in ("M", "F"):
            continue

        age = calculate_age(dob, as_of=benefit_year_end)
        model = get_model_type(age)
        demo_var = get_demographic_variable(model, gender, age)
        demo_coef = get_coefficient(aca_coefficients, model, demo_var, metal_level)

        results.append({
            "member_id": row["member_id"],
            "date_of_birth": dob,
            "gender": gender,
            "metal_level": metal_level,
            "enrollment_months": row["enrollment_months"],
            "diagnoses": row["diagnoses"],
            "ndc_codes": row["ndc_codes"],
            "age": age,
            "model": model,
            "demographic_variable": demo_var,
            "demographic_coefficient": demo_coef,
        })

    df = pl.DataFrame(results)

    # Model distribution
    model_counts = df.group_by("model").count().to_dicts()

    context.log.info(f"Calculated demographics for {len(df)} members")
    context.add_output_metadata({
        "member_count": len(df),
        "model_distribution": {r["model"]: r["count"] for r in model_counts},
    })

    return df


# =============================================================================
# Step 3: Raw CCs (ICD → CC Mapping)
# =============================================================================


@asset(
    group_name="aca_scoring_detailed",
    description="Maps ICD-10 diagnosis codes to Condition Categories (CCs) using "
    "CMS Table 3. This is the raw mapping before hierarchy application.",
    ins={"aca_demographics": AssetIn()},
)
def aca_raw_ccs(
    context: AssetExecutionContext,
    aca_demographics: pl.DataFrame,
    aca_icd_to_cc_mapping: dict[str, list[str]],
) -> pl.DataFrame:
    """Map diagnoses to raw CCs."""
    context.log.info("Mapping diagnoses to Condition Categories")

    results = []
    total_ccs = 0

    for row in aca_demographics.iter_rows(named=True):
        diagnoses = row["diagnoses"] or []
        raw_ccs = map_diagnoses_to_ccs(diagnoses, aca_icd_to_cc_mapping)
        total_ccs += len(raw_ccs)

        results.append({
            **{k: v for k, v in row.items() if k != "diagnoses"},
            "diagnoses": diagnoses,
            "raw_ccs": sorted(raw_ccs),
            "raw_cc_count": len(raw_ccs),
        })

    df = pl.DataFrame(results)

    context.log.info(f"Mapped {total_ccs} total CCs for {len(df)} members")
    context.add_output_metadata({
        "member_count": len(df),
        "total_raw_ccs": total_ccs,
        "avg_ccs_per_member": round(total_ccs / len(df), 2) if len(df) > 0 else 0,
    })

    return df


# =============================================================================
# Step 4: HCCs After Hierarchy
# =============================================================================


@asset(
    group_name="aca_scoring_detailed",
    description="Applies HCC hierarchies to remove superseded conditions. For example, "
    "if a member has both severe and mild diabetes, only the severe one counts.",
    ins={"aca_raw_ccs": AssetIn()},
)
def aca_hccs_post_hierarchy(
    context: AssetExecutionContext,
    aca_raw_ccs: pl.DataFrame,
    aca_hcc_hierarchies: dict[str, list[str]],
) -> pl.DataFrame:
    """Apply HCC hierarchies."""
    context.log.info("Applying HCC hierarchies")

    results = []
    total_removed = 0

    for row in aca_raw_ccs.iter_rows(named=True):
        raw_ccs = set(row["raw_ccs"])
        hccs_after = apply_hcc_hierarchies(raw_ccs, aca_hcc_hierarchies)
        removed = raw_ccs - hccs_after
        total_removed += len(removed)

        results.append({
            **{k: v for k, v in row.items()},
            "hccs_post_hierarchy": sorted(hccs_after),
            "hccs_removed_by_hierarchy": sorted(removed),
        })

    df = pl.DataFrame(results)

    context.log.info(f"Removed {total_removed} HCCs via hierarchy rules")
    context.add_output_metadata({
        "member_count": len(df),
        "total_hccs_removed": total_removed,
    })

    return df


# =============================================================================
# Step 5: Final HCCs (Exclusions + Groupings)
# =============================================================================


@asset(
    group_name="aca_scoring_detailed",
    description="Applies model-specific exclusions and HCC groupings. Some HCCs are "
    "excluded from certain models, and some are grouped (e.g., diabetes G01).",
    ins={"aca_hccs_post_hierarchy": AssetIn()},
)
def aca_hccs_final(
    context: AssetExecutionContext,
    aca_hccs_post_hierarchy: pl.DataFrame,
    aca_model_exclusions: dict[str, set[str]],
    aca_hcc_groups_adult: dict[str, list[str]],
    aca_hcc_groups_child: dict[str, list[str]],
) -> pl.DataFrame:
    """Apply model exclusions and groupings."""
    context.log.info("Applying model exclusions and HCC groupings")

    results = []
    total_groups_triggered = 0

    for row in aca_hccs_post_hierarchy.iter_rows(named=True):
        model = row["model"]
        hccs = set(row["hccs_post_hierarchy"])

        # Apply model exclusions
        hccs_filtered = exclude_model_hccs(hccs, model, aca_model_exclusions)

        # Apply groupings
        if model == "Adult":
            groups = aca_hcc_groups_adult
        elif model == "Child":
            groups = aca_hcc_groups_child
        else:
            groups = {}

        remaining_hccs, triggered_groups = apply_hcc_groupings(hccs_filtered, model, groups)
        total_groups_triggered += len(triggered_groups)

        results.append({
            **{k: v for k, v in row.items()},
            "hccs_filtered": sorted(hccs_filtered),
            "remaining_hccs": sorted(remaining_hccs),
            "triggered_groups": sorted(triggered_groups),
            "hcc_count": len(remaining_hccs) + len(triggered_groups),
        })

    df = pl.DataFrame(results)

    context.log.info(f"Triggered {total_groups_triggered} HCC groups")
    context.add_output_metadata({
        "member_count": len(df),
        "total_groups_triggered": total_groups_triggered,
    })

    return df


# =============================================================================
# Step 6: RXCs
# =============================================================================


@asset(
    group_name="aca_scoring_detailed",
    description="Maps NDC codes to RXC categories and applies RXC hierarchies.",
    ins={"aca_hccs_final": AssetIn()},
)
def aca_rxcs(
    context: AssetExecutionContext,
    aca_hccs_final: pl.DataFrame,
    aca_ndc_to_rxc_mapping: dict[str, list[str]],
    aca_rxc_hierarchies: dict[str, list[str]],
) -> pl.DataFrame:
    """Map NDCs to RXCs and apply hierarchies."""
    context.log.info("Mapping NDCs to RXCs")

    results = []
    total_rxcs = 0

    for row in aca_hccs_final.iter_rows(named=True):
        ndc_codes = row["ndc_codes"] or []
        raw_rxcs = map_ndcs_to_rxcs(ndc_codes, aca_ndc_to_rxc_mapping)
        rxcs_after = apply_rxc_hierarchies(raw_rxcs, aca_rxc_hierarchies)
        total_rxcs += len(rxcs_after)

        results.append({
            **{k: v for k, v in row.items() if k != "ndc_codes"},
            "ndc_codes": ndc_codes,
            "raw_rxcs": sorted(raw_rxcs),
            "rxcs_final": sorted(rxcs_after),
            "rxc_count": len(rxcs_after),
        })

    df = pl.DataFrame(results)

    context.log.info(f"Mapped {total_rxcs} total RXCs for {len(df)} members")
    context.add_output_metadata({
        "member_count": len(df),
        "total_rxcs": total_rxcs,
    })

    return df


# =============================================================================
# Step 7: Final Scores
# =============================================================================


@asset(
    group_name="aca_scoring_detailed",
    description="Calculates final risk scores by summing demographic, HCC, RXC, and "
    "EDF coefficients. This is the final output of the detailed scoring pipeline.",
    ins={"aca_rxcs": AssetIn()},
)
def aca_scores(
    context: AssetExecutionContext,
    config: ScoringConfig,
    aca_rxcs: pl.DataFrame,
    aca_coefficients: dict[tuple[str, str], dict[str, float]],
) -> pl.DataFrame:
    """Calculate final risk scores."""
    context.log.info("Calculating final risk scores")

    results = []

    for row in aca_rxcs.iter_rows(named=True):
        model = row["model"]
        metal_level = row["metal_level"]
        remaining_hccs = set(row["remaining_hccs"])
        triggered_groups = set(row["triggered_groups"])
        rxcs_final = set(row["rxcs_final"])
        enrollment_months = row["enrollment_months"] or 12
        hcc_count = row["hcc_count"]

        demographic_coef = row["demographic_coefficient"]

        # Calculate HCC scores
        hcc_score, hcc_details = calculate_hcc_scores(
            remaining_hccs, triggered_groups, aca_coefficients, model, metal_level
        )

        # Calculate EDF (Adult only)
        edf_var, edf_factor = calculate_edf(
            model, enrollment_months, hcc_count, aca_coefficients, metal_level
        )
        if edf_factor != 0.0:
            hcc_score += edf_factor

        # Calculate RXC scores
        rxc_score, rxc_details = calculate_rxc_scores(
            rxcs_final, aca_coefficients, model, metal_level
        )

        # Total score
        total_score = calculate_total_score(demographic_coef, hcc_score, rxc_score)

        results.append({
            "member_id": row["member_id"],
            "risk_score": round(total_score, 6),
            "demographic_score": round(demographic_coef, 6),
            "hcc_score": round(hcc_score, 6),
            "rxc_score": round(rxc_score, 6),
            "edf_factor": round(edf_factor, 6),
            "model": model,
            "age": row["age"],
            "gender": row["gender"],
            "metal_level": metal_level,
            "enrollment_months": enrollment_months,
            "remaining_hccs": sorted(remaining_hccs),
            "triggered_groups": sorted(triggered_groups),
            "rxcs_final": sorted(rxcs_final),
            "hcc_count": hcc_count,
            "rxc_count": row["rxc_count"],
        })

    df = pl.DataFrame(results)

    # Summary statistics
    avg_score = df["risk_score"].mean()
    min_score = df["risk_score"].min()
    max_score = df["risk_score"].max()

    context.log.info(
        f"Scored {len(df)} members. "
        f"Avg={avg_score:.3f}, Min={min_score:.3f}, Max={max_score:.3f}"
    )
    context.add_output_metadata({
        "member_count": len(df),
        "avg_risk_score": round(avg_score, 4) if avg_score else 0,
        "min_risk_score": round(min_score, 4) if min_score else 0,
        "max_risk_score": round(max_score, 4) if max_score else 0,
        "model_distribution": df.group_by("model").count().to_dicts(),
    })

    return df
