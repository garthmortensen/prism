"""Pure functions for ACA HHS-HCC risk score calculation.

These functions are extracted from ACACalculator to enable:
1. Dagster asset-based orchestration with granular DAG visibility
2. Independent unit testing of each scoring step
3. Reuse across different execution contexts

Each function represents a discrete step in the risk scoring pipeline:
- Demographics: age calculation, model selection, demographic factors
- HCC Processing: ICD→CC mapping, hierarchy application, groupings
- RXC Processing: NDC→RXC mapping, RXC hierarchy application
- Scoring: coefficient lookup and summation
"""

from datetime import date
from typing import Any


# =============================================================================
# Demographics Functions
# =============================================================================


def calculate_age(dob: date, as_of: date | None = None) -> int:
    """Calculate age in years as of a given date.

    Args:
        dob: Date of birth
        as_of: Date to calculate age as of (defaults to today)

    Returns:
        Age in completed years
    """
    as_of = as_of or date.today()
    age = as_of.year - dob.year
    if (as_of.month, as_of.day) < (dob.month, dob.day):
        age -= 1
    return max(0, age)


def get_model_type(age: int) -> str:
    """Determine model type from age.

    Args:
        age: Age in years

    Returns:
        "Adult", "Child", or "Infant"
    """
    if age >= 21:
        return "Adult"
    elif age >= 2:
        return "Child"
    else:
        return "Infant"


def get_demographic_variable(model: str, gender: str, age: int) -> str:
    """Get the demographic variable name for coefficient lookup.

    Args:
        model: Model type ("Adult", "Child", "Infant")
        gender: "M" or "F"
        age: Age in years

    Returns:
        Variable name (e.g., "MAGE_LAST_55_59")
    """
    prefix = "M" if gender == "M" else "F"

    if model == "Adult":
        if age <= 24:
            return f"{prefix}AGE_LAST_21_24"
        elif age <= 29:
            return f"{prefix}AGE_LAST_25_29"
        elif age <= 34:
            return f"{prefix}AGE_LAST_30_34"
        elif age <= 39:
            return f"{prefix}AGE_LAST_35_39"
        elif age <= 44:
            return f"{prefix}AGE_LAST_40_44"
        elif age <= 49:
            return f"{prefix}AGE_LAST_45_49"
        elif age <= 54:
            return f"{prefix}AGE_LAST_50_54"
        elif age <= 59:
            return f"{prefix}AGE_LAST_55_59"
        else:
            return f"{prefix}AGE_LAST_60_GT"

    elif model == "Child":
        if age <= 4:
            return f"{prefix}AGE_LAST_2_4"
        elif age <= 9:
            return f"{prefix}AGE_LAST_5_9"
        elif age <= 14:
            return f"{prefix}AGE_LAST_10_14"
        else:
            return f"{prefix}AGE_LAST_15_20"

    else:  # Infant
        return f"{prefix}AGE_LAST_0_1"


# =============================================================================
# Coefficient Lookup Functions
# =============================================================================


def get_coefficient(
    coefficients: dict[tuple[str, str], dict[str, float]],
    model: str,
    variable: str,
    metal_level: str,
) -> float:
    """Look up coefficient for a variable.

    Args:
        coefficients: Coefficient table from load_coefficients()
        model: Model type ("Adult", "Child", "Infant")
        variable: Variable name (e.g., "HHS_HCC001", "MAGE_LAST_55_59")
        metal_level: Metal level (platinum, gold, silver, bronze, catastrophic)

    Returns:
        Coefficient value, or 0.0 if not found
    """
    key = (model, variable)
    if key not in coefficients:
        return 0.0

    metal = metal_level.lower()
    return coefficients[key].get(metal, 0.0)


# =============================================================================
# HCC Processing Functions
# =============================================================================


def map_diagnoses_to_ccs(
    diagnoses: list[str],
    icd_to_cc: dict[str, list[str]],
) -> set[str]:
    """Map ICD-10 codes to Condition Categories.

    Args:
        diagnoses: List of ICD-10-CM diagnosis codes
        icd_to_cc: ICD to CC mapping table from load_icd_to_cc()

    Returns:
        Set of CC numbers (as strings, e.g., "19", "35_1")
    """
    ccs: set[str] = set()

    for dx in diagnoses:
        # Normalize: remove dots, uppercase
        dx_clean = dx.replace(".", "").upper()

        # Try exact match first
        if dx_clean in icd_to_cc:
            ccs.update(icd_to_cc[dx_clean])
            continue

        # Try progressively shorter prefixes (handle code variations)
        for length in [6, 5, 4, 3]:
            prefix = dx_clean[:length]
            if prefix in icd_to_cc:
                ccs.update(icd_to_cc[prefix])
                break

    # Normalize CCs to match hierarchy and coefficient table formats
    # e.g., "21.0" -> "21", "35.1" -> "35_1"
    normalized_ccs: set[str] = set()
    for cc in ccs:
        # Remove .0 suffix
        if cc.endswith(".0"):
            cc = cc[:-2]
        # Replace remaining dots with underscores
        cc = cc.replace(".", "_")
        normalized_ccs.add(cc)

    return normalized_ccs


def apply_hcc_hierarchies(
    cc_set: set[str],
    hierarchies: dict[str, list[str]],
) -> set[str]:
    """Apply HCC hierarchies to remove superseded conditions.

    Args:
        cc_set: Set of condition categories before hierarchy application
        hierarchies: Hierarchy rules from load_hierarchies()

    Returns:
        Set of HCCs after removing superseded conditions
    """
    hcc_set = cc_set.copy()

    for dominant, superseded_list in hierarchies.items():
        if dominant in hcc_set:
            for superseded in superseded_list:
                hcc_set.discard(superseded)

    return hcc_set


def exclude_model_hccs(
    hccs: set[str],
    model: str,
    exclusions: dict[str, set[str]],
) -> set[str]:
    """Remove HCCs that are excluded from this model.

    Args:
        hccs: Set of HCCs
        model: Model type
        exclusions: Model exclusions from load_model_exclusions()

    Returns:
        HCCs with model-specific exclusions removed
    """
    excluded = exclusions.get(model, set())
    return hccs - excluded


def apply_hcc_groupings(
    hccs: set[str],
    model: str,
    groups: dict[str, list[str]],
) -> tuple[set[str], set[str]]:
    """Apply HCC groupings for the model.

    Some HCCs are grouped together (e.g., G01 = HCC 19, 20, 21).
    When any HCC in a group is present, the group variable is set
    and individual HCCs are zeroed.

    Args:
        hccs: Set of HCCs after hierarchy application
        model: Model type ("Adult", "Child")
        groups: HCC groups from load_hcc_groups()

    Returns:
        Tuple of (remaining HCCs, group variables triggered)
    """
    if model == "Infant":
        # Infant model uses severity levels, not groups
        return hccs, set()

    remaining_hccs = hccs.copy()
    triggered_groups: set[str] = set()

    for group_var, group_hccs in groups.items():
        # Check if any HCC in this group is present
        for hcc in group_hccs:
            if hcc in remaining_hccs:
                triggered_groups.add(group_var)
                # Remove all HCCs in this group from individual scoring
                for h in group_hccs:
                    remaining_hccs.discard(h)
                break

    return remaining_hccs, triggered_groups


def format_hcc_variable_name(hcc: str) -> str:
    """Format an HCC code into a coefficient variable name.

    Args:
        hcc: HCC code (e.g., "19", "35_1")

    Returns:
        Variable name (e.g., "HHS_HCC019", "HHS_HCC035_1")
    """
    if hcc.isdigit():
        return f"HHS_HCC{hcc.zfill(3)}"
    else:
        # Handle cases like "35_1" -> "HHS_HCC035_1"
        parts = hcc.split("_")
        if len(parts) == 2 and parts[0].isdigit():
            return f"HHS_HCC{parts[0].zfill(3)}_{parts[1]}"
        else:
            return f"HHS_HCC{hcc}"


# =============================================================================
# RXC Processing Functions
# =============================================================================


def map_ndcs_to_rxcs(
    ndc_codes: list[str],
    ndc_to_rxc: dict[str, list[str]],
) -> set[str]:
    """Map NDC codes to RXCs.

    Args:
        ndc_codes: List of NDC codes
        ndc_to_rxc: NDC to RXC mapping from load_ndc_to_rxc()

    Returns:
        Set of RXC numbers (as strings)
    """
    rxcs: set[str] = set()

    for ndc in ndc_codes:
        # Normalize: remove dashes, strip whitespace
        ndc_clean = ndc.replace("-", "").strip()

        if ndc_clean in ndc_to_rxc:
            rxcs.update(ndc_to_rxc[ndc_clean])
        # Try padding to 11 digits if it's numeric and shorter
        elif ndc_clean.isdigit() and len(ndc_clean) < 11:
            padded = ndc_clean.zfill(11)
            if padded in ndc_to_rxc:
                rxcs.update(ndc_to_rxc[padded])

    return rxcs


def apply_rxc_hierarchies(
    rxcs: set[str],
    rxc_hierarchies: dict[str, list[str]],
) -> set[str]:
    """Apply RXC hierarchies.

    Args:
        rxcs: Set of RXCs
        rxc_hierarchies: RXC hierarchy rules from load_rxc_hierarchies()

    Returns:
        Set of RXCs after hierarchy application
    """
    final_rxcs = rxcs.copy()
    for dominant, superseded_list in rxc_hierarchies.items():
        if dominant in final_rxcs:
            for superseded in superseded_list:
                final_rxcs.discard(superseded)
    return final_rxcs


def format_rxc_variable_name(
    rxc: str,
    coefficients: dict[tuple[str, str], dict[str, float]],
    model: str,
    metal_level: str,
) -> tuple[str, float]:
    """Find the correct variable name format for an RXC and its coefficient.

    Args:
        rxc: RXC code (e.g., "1", "01")
        coefficients: Coefficient table
        model: Model type
        metal_level: Metal level

    Returns:
        Tuple of (variable name, coefficient value)
    """
    candidates = [
        f"RXC_{rxc.zfill(2)}",
        f"RXC_{rxc.zfill(3)}",
        f"RXC_{rxc}",
        rxc,
    ]

    for candidate in candidates:
        coef = get_coefficient(coefficients, model, candidate, metal_level)
        if coef != 0.0:
            return candidate, coef

    return f"RXC_{rxc.zfill(2)}", 0.0


# =============================================================================
# Enrollment Duration Factor (EDF) Functions
# =============================================================================


def calculate_edf(
    model: str,
    enrollment_months: int,
    hcc_count: int,
    coefficients: dict[tuple[str, str], dict[str, float]],
    metal_level: str,
) -> tuple[str | None, float]:
    """Calculate Enrollment Duration Factor for Adult model.

    Per HHS/HCC EDF logic: if ENROLDURATION = N and HCC_CNT > 0 then HCC_EDN = 1

    Args:
        model: Model type
        enrollment_months: Number of enrollment months (1-12)
        hcc_count: Count of payment HCCs + triggered groups
        coefficients: Coefficient table
        metal_level: Metal level

    Returns:
        Tuple of (EDF variable name or None, EDF coefficient)
    """
    if model != "Adult":
        return None, 0.0

    if not (1 <= enrollment_months <= 6 and hcc_count > 0):
        return None, 0.0

    edf_var = f"HCC_ED{enrollment_months}"
    edf_factor = get_coefficient(coefficients, model, edf_var, metal_level)

    return edf_var, edf_factor


# =============================================================================
# Source Tracing Functions
# =============================================================================


def find_source_icds_for_hcc(
    hcc: str,
    diagnoses: list[str],
    icd_to_cc: dict[str, list[str]],
) -> list[str]:
    """Find which ICD codes contributed to a specific HCC.

    Args:
        hcc: HCC code (e.g., "19", "35_1")
        diagnoses: List of ICD-10 diagnosis codes
        icd_to_cc: ICD to CC mapping table

    Returns:
        List of ICD codes that mapped to this HCC
    """
    source_icds = []
    for dx in diagnoses:
        dx_ccs = map_diagnoses_to_ccs([dx], icd_to_cc)
        if hcc in dx_ccs:
            source_icds.append(dx)
    return source_icds


def find_source_ndcs_for_rxc(
    rxc: str,
    ndc_codes: list[str],
    ndc_to_rxc: dict[str, list[str]],
) -> list[str]:
    """Find which NDC codes contributed to a specific RXC.

    Args:
        rxc: RXC code (e.g., "1", "01")
        ndc_codes: List of NDC codes
        ndc_to_rxc: NDC to RXC mapping table

    Returns:
        List of NDC codes that mapped to this RXC
    """
    source_ndcs = []
    for ndc in ndc_codes:
        ndc_rxcs = map_ndcs_to_rxcs([ndc], ndc_to_rxc)
        if rxc in ndc_rxcs:
            source_ndcs.append(ndc)
    return source_ndcs


# =============================================================================
# Score Aggregation Functions
# =============================================================================


def calculate_hcc_scores(
    remaining_hccs: set[str],
    triggered_groups: set[str],
    coefficients: dict[tuple[str, str], dict[str, float]],
    model: str,
    metal_level: str,
) -> tuple[float, dict[str, float]]:
    """Calculate total HCC score from remaining HCCs and triggered groups.

    Args:
        remaining_hccs: HCCs not absorbed by groups
        triggered_groups: Group variables triggered
        coefficients: Coefficient table
        model: Model type
        metal_level: Metal level

    Returns:
        Tuple of (total HCC score, dict of variable->coefficient)
    """
    hcc_score = 0.0
    hcc_details: dict[str, float] = {}

    # Score individual HCCs
    for hcc in remaining_hccs:
        var_name = format_hcc_variable_name(hcc)
        coef = get_coefficient(coefficients, model, var_name, metal_level)
        if coef != 0.0:
            hcc_score += coef
            hcc_details[var_name] = coef

    # Score group variables
    for group in triggered_groups:
        coef = get_coefficient(coefficients, model, group, metal_level)
        if coef != 0.0:
            hcc_score += coef
            hcc_details[group] = coef

    return hcc_score, hcc_details


def calculate_rxc_scores(
    rxcs_after_hierarchy: set[str],
    coefficients: dict[tuple[str, str], dict[str, float]],
    model: str,
    metal_level: str,
) -> tuple[float, dict[str, float]]:
    """Calculate total RXC score.

    Args:
        rxcs_after_hierarchy: RXCs after hierarchy application
        coefficients: Coefficient table
        model: Model type
        metal_level: Metal level

    Returns:
        Tuple of (total RXC score, dict of variable->coefficient)
    """
    rxc_score = 0.0
    rxc_details: dict[str, float] = {}

    for rxc in rxcs_after_hierarchy:
        var_name, coef = format_rxc_variable_name(rxc, coefficients, model, metal_level)
        if coef != 0.0:
            rxc_score += coef
            rxc_details[var_name] = coef

    return rxc_score, rxc_details


def calculate_total_score(
    demographic_factor: float,
    hcc_score: float,
    rxc_score: float,
    edf_factor: float = 0.0,
) -> float:
    """Calculate total risk score.

    Args:
        demographic_factor: Demographic coefficient
        hcc_score: Total HCC/group score
        rxc_score: Total RXC score
        edf_factor: Enrollment duration factor (if applicable)

    Returns:
        Total risk score
    """
    return demographic_factor + hcc_score + rxc_score + edf_factor
