"""Dagster assets for ACA HHS-HCC reference data.

These assets load and cache the official CMS DIY tables (ICD→CC mappings,
hierarchies, coefficients, etc.). They are designed to be materialized once
per model year and cached for reuse by downstream scoring assets.

Asset Dependency Graph:
    aca_icd_to_cc_mapping ─────┐
    aca_hcc_hierarchies ───────┤
    aca_coefficients ──────────┼──► aca_scoring assets
    aca_model_exclusions ──────┤
    aca_hcc_groups_adult ──────┤
    aca_hcc_groups_child ──────┤
    aca_ndc_to_rxc_mapping ────┤
    aca_rxc_hierarchies ───────┘
"""

from typing import Any

from dagster import (
    AssetExecutionContext,
    asset,
)

from ra_calculators.aca_risk_score_calculator.table_loader import (
    load_coefficients,
    load_hcc_groups,
    load_hcc_labels,
    load_hierarchies,
    load_icd_to_cc,
    load_model_exclusions,
    load_ndc_to_rxc,
    load_rxc_hierarchies,
    load_rxc_labels,
)


# Default model year - can be overridden via run config
DEFAULT_MODEL_YEAR = "2024"


# =============================================================================
# ICD to CC Mapping
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="ICD-10 to Condition Category (CC) mappings from CMS Table 3. "
    "Maps diagnosis codes to one or more CCs for risk scoring.",

    metadata={"table_source": "table_3.parquet"},
)
def aca_icd_to_cc_mapping(
    context: AssetExecutionContext,
) -> dict[str, list[str]]:
    """Load ICD-10 to CC mappings from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading ICD→CC mapping for model year {model_year}")

    icd_to_cc = load_icd_to_cc(model_year)

    context.log.info(f"Loaded {len(icd_to_cc)} ICD-10 codes")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "icd_code_count": len(icd_to_cc),
            "sample_mappings": dict(list(icd_to_cc.items())[:5]),
        }
    )

    return icd_to_cc


# =============================================================================
# HCC Hierarchies
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="HCC hierarchy rules from CMS Table 4. Defines which conditions "
    "supersede others (e.g., more severe diabetes trumps less severe).",

    metadata={"table_source": "table_4.parquet"},
)
def aca_hcc_hierarchies(
    context: AssetExecutionContext,
) -> dict[str, list[str]]:
    """Load HCC hierarchies from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading HCC hierarchies for model year {model_year}")

    hierarchies = load_hierarchies(model_year)

    context.log.info(f"Loaded {len(hierarchies)} hierarchy rules")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "hierarchy_count": len(hierarchies),
            "sample_hierarchies": dict(list(hierarchies.items())[:5]),
        }
    )

    return hierarchies


# =============================================================================
# Coefficients
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="Risk coefficients from CMS Table 9. Contains coefficients for "
    "demographics, HCCs, RXCs, and groups by model type and metal level.",

    metadata={"table_source": "table_9.parquet"},
)
def aca_coefficients(
    context: AssetExecutionContext,
) -> dict[tuple[str, str], dict[str, float]]:
    """Load risk coefficients from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading coefficients for model year {model_year}")

    coefficients = load_coefficients(model_year)

    # Count by model type
    model_counts = {}
    for (model, _var), _coefs in coefficients.items():
        model_counts[model] = model_counts.get(model, 0) + 1

    context.log.info(f"Loaded {len(coefficients)} coefficient entries")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "total_coefficients": len(coefficients),
            "coefficients_by_model": model_counts,
        }
    )

    return coefficients


# =============================================================================
# Model Exclusions
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="Model-specific HCC exclusions from CMS Table 12. Some HCCs are "
    "excluded from certain models (Adult, Child, Infant).",

    metadata={"table_source": "table_12.parquet"},
)
def aca_model_exclusions(
    context: AssetExecutionContext,
) -> dict[str, set[str]]:
    """Load model exclusions from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading model exclusions for model year {model_year}")

    exclusions = load_model_exclusions(model_year)

    context.log.info(
        f"Loaded exclusions: Adult={len(exclusions.get('Adult', set()))}, "
        f"Child={len(exclusions.get('Child', set()))}, "
        f"Infant={len(exclusions.get('Infant', set()))}"
    )
    context.add_output_metadata(
        {
            "model_year": model_year,
            "adult_exclusions": len(exclusions.get("Adult", set())),
            "child_exclusions": len(exclusions.get("Child", set())),
            "infant_exclusions": len(exclusions.get("Infant", set())),
        }
    )

    return exclusions


# =============================================================================
# HCC Groups
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="Adult HCC groupings from CMS Table 6. Some HCCs are grouped "
    "(e.g., G01 = diabetes HCCs 19, 20, 21) and scored as a unit.",

    metadata={"table_source": "table_6.json"},
)
def aca_hcc_groups_adult(
    context: AssetExecutionContext,
) -> dict[str, list[str]]:
    """Load Adult HCC groups from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading Adult HCC groups for model year {model_year}")

    groups = load_hcc_groups(model_year, "Adult")

    context.log.info(f"Loaded {len(groups)} Adult HCC groups")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "group_count": len(groups),
            "groups": list(groups.keys()),
        }
    )

    return groups


@asset(
    group_name="aca_reference_data",
    description="Child HCC groupings from CMS Table 7. Some HCCs are grouped "
    "and scored as a unit in the Child model.",

    metadata={"table_source": "table_7.json"},
)
def aca_hcc_groups_child(
    context: AssetExecutionContext,
) -> dict[str, list[str]]:
    """Load Child HCC groups from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading Child HCC groups for model year {model_year}")

    groups = load_hcc_groups(model_year, "Child")

    context.log.info(f"Loaded {len(groups)} Child HCC groups")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "group_count": len(groups),
            "groups": list(groups.keys()),
        }
    )

    return groups


# =============================================================================
# NDC to RXC Mapping
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="NDC to RXC mappings from CMS Table 10a. Maps prescription drug "
    "codes to RXC categories for risk scoring.",

    metadata={"table_source": "table_10a.parquet"},
)
def aca_ndc_to_rxc_mapping(
    context: AssetExecutionContext,
) -> dict[str, list[str]]:
    """Load NDC to RXC mappings from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading NDC→RXC mapping for model year {model_year}")

    ndc_to_rxc = load_ndc_to_rxc(model_year)

    context.log.info(f"Loaded {len(ndc_to_rxc)} NDC codes")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "ndc_code_count": len(ndc_to_rxc),
        }
    )

    return ndc_to_rxc


# =============================================================================
# RXC Hierarchies
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="RXC hierarchy rules from CMS Table 11. Defines which drug "
    "categories supersede others.",

    metadata={"table_source": "table_11.parquet"},
)
def aca_rxc_hierarchies(
    context: AssetExecutionContext,
) -> dict[str, list[str]]:
    """Load RXC hierarchies from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading RXC hierarchies for model year {model_year}")

    hierarchies = load_rxc_hierarchies(model_year)

    context.log.info(f"Loaded {len(hierarchies)} RXC hierarchy rules")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "hierarchy_count": len(hierarchies),
        }
    )

    return hierarchies


# =============================================================================
# Labels (Optional - for UI display)
# =============================================================================


@asset(
    group_name="aca_reference_data",
    description="HCC labels from CMS Table 4. Human-readable descriptions for HCCs.",

    metadata={"table_source": "table_4.parquet"},
)
def aca_hcc_labels(
    context: AssetExecutionContext,
) -> dict[str, str]:
    """Load HCC labels from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading HCC labels for model year {model_year}")

    labels = load_hcc_labels(model_year)

    context.log.info(f"Loaded {len(labels)} HCC labels")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "label_count": len(labels),
        }
    )

    return labels


@asset(
    group_name="aca_reference_data",
    description="RXC labels from CMS Table 10a. Human-readable descriptions for RXCs.",

    metadata={"table_source": "table_10a.parquet"},
)
def aca_rxc_labels(
    context: AssetExecutionContext,
) -> dict[str, str]:
    """Load RXC labels from CMS DIY tables."""
    model_year = context.run_config.get("model_year", DEFAULT_MODEL_YEAR)
    context.log.info(f"Loading RXC labels for model year {model_year}")

    labels = load_rxc_labels(model_year)

    context.log.info(f"Loaded {len(labels)} RXC labels")
    context.add_output_metadata(
        {
            "model_year": model_year,
            "label_count": len(labels),
        }
    )

    return labels
