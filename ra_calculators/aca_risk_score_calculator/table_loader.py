"""Load DIY tables from CMS HHS-HCC model files.

This module loads the official CMS lookup tables from:
    diy_tables/cy202*_diy_tables/

Tables loaded (parquet format for performance):
    - table_3.parquet: ICD-10 to CC mappings
    - table_4.parquet: HCC hierarchies
    - table_9.parquet: Risk coefficients by metal level
    - table_6.json: Adult HCC groupings
    - table_7.json: Child HCC groupings
    - table_12.parquet: Model-specific HCC exclusions
"""

import json
from pathlib import Path
from typing import Any

import polars as pl

# Base directory for DIY tables
DATA_DIR = Path(__file__).parent / "diy_tables"

# Cache loaded tables
_CACHE: dict[str, dict[str, Any]] = {}


def _get_tables_dir(model_year: str) -> Path:
    """Get the directory for a model year's tables."""
    tables_dir = DATA_DIR / f"cy{model_year}_diy_tables"
    if not tables_dir.exists():
        raise FileNotFoundError(
            f"DIY tables not found for model year {model_year}. Expected directory: {tables_dir}"
        )
    return tables_dir


def load_icd_to_cc(model_year: str = "2024") -> dict[str, list[str]]:
    """Load ICD-10 to CC mappings from table_3.parquet.

    Args:
        model_year: Model year (e.g., "2024")

    Returns:
        Dictionary mapping ICD-10 codes to list of CCs
        (some diagnoses map to multiple CCs)
    """
    cache_key = f"icd_to_cc_{model_year}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tables_dir = _get_tables_dir(model_year)
    icd_to_cc: dict[str, list[str]] = {}

    df = pl.read_parquet(tables_dir / "table_3.parquet")
    for row in df.iter_rows(named=True):
        icd10 = str(row["icd10"]).strip()
        ccs = []

        # Primary CC
        if row.get("cc") and str(row["cc"]).strip():
            ccs.append(str(row["cc"]).strip())

        # Some diagnoses map to multiple CCs
        if row.get("second_cc") and str(row["second_cc"]).strip():
            ccs.append(str(row["second_cc"]).strip())
        if row.get("third_cc") and str(row["third_cc"]).strip():
            ccs.append(str(row["third_cc"]).strip())

        if ccs:
            icd_to_cc[icd10] = ccs

    _CACHE[cache_key] = icd_to_cc
    return icd_to_cc


def load_hierarchies(model_year: str = "2024") -> dict[str, list[str]]:
    """Load HCC hierarchies from table_4.parquet.

    Args:
        model_year: Model year (e.g., "2024")

    Returns:
        Dictionary mapping dominant HCC to list of HCCs it supersedes
    """
    cache_key = f"hierarchies_{model_year}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tables_dir = _get_tables_dir(model_year)
    hierarchies: dict[str, list[str]] = {}

    df = pl.read_parquet(tables_dir / "table_4.parquet")

    # Determine HCC column name (e.g., v07_hcc for 2024, v08_hcc for 2025)
    hcc_col = next((col for col in df.columns if col.endswith("_hcc")), "v07_hcc")

    for row in df.iter_rows(named=True):
        hcc = str(row[hcc_col]).strip()
        zeros = str(row.get("hccs_to_zero", "") or "").strip()

        if zeros:
            # Parse comma-separated list, handling spaces
            superseded = [h.strip() for h in zeros.split(",") if h.strip()]
            if superseded:
                hierarchies[hcc] = superseded

    _CACHE[cache_key] = hierarchies
    return hierarchies


def load_coefficients(model_year: str = "2024") -> dict[tuple[str, str], dict[str, float]]:
    """Load risk coefficients from table_9.parquet.

    Args:
        model_year: Model year (e.g., "2024")

    Returns:
        Dictionary mapping (model, variable) to metal-level coefficients
        e.g., {("Adult", "HHS_HCC001"): {"platinum": 0.61, "gold": 0.495, ...}}
    """
    cache_key = f"coefficients_{model_year}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tables_dir = _get_tables_dir(model_year)
    coefficients: dict[tuple[str, str], dict[str, float]] = {}

    df = pl.read_parquet(tables_dir / "table_9.parquet")
    for row in df.iter_rows(named=True):
        model = str(row["model"]).strip()
        variable = str(row["variable"]).strip()

        coefficients[(model, variable)] = {
            "platinum": float(row["platinum_level"]),
            "gold": float(row["gold_level"]),
            "silver": float(row["silver_level"]),
            "bronze": float(row["bronze_level"]),
            "catastrophic": float(row["catastrophic_level"]),
        }

    _CACHE[cache_key] = coefficients
    return coefficients


def load_hcc_groups(model_year: str = "2024", model: str = "Adult") -> dict[str, list[str]]:
    """Load HCC groupings from table_6.json (Adult) or table_7.json (Child).

    Some HCCs are grouped together (e.g., G01 = HCC 19, 20, 21 for diabetes).

    Args:
        model_year: Model year (e.g., "2024")
        model: "Adult" or "Child"

    Returns:
        Dictionary mapping group variable to list of HCCs in that group
    """
    cache_key = f"groups_{model_year}_{model}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tables_dir = _get_tables_dir(model_year)
    table_file = "table_6.json" if model == "Adult" else "table_7.json"

    groups: dict[str, list[str]] = {}

    with open(tables_dir / table_file, encoding="utf-8") as f:
        data = json.load(f)

    for item in data:
        if item.get("model") != model:
            continue

        variable = item.get("variable", "").strip()
        if not variable:
            continue

        # Only include groups that replace HCCs (contain "= 0")
        # This excludes interaction variables like SEVERE, TRANSPLANT, etc.
        # which do not zero out the underlying HCCs.
        is_replacement = False
        definition = item.get("definition", "")
        if "= 0" in definition:
            is_replacement = True
        
        for cont in item.get("continuation", []):
            if "= 0" in cont.get("definition", ""):
                is_replacement = True
        
        if not is_replacement:
            continue

        # Parse HCCs from definition and continuation
        hccs = []
        if definition:
            hccs.extend(_extract_hccs_from_definition(definition))

        for cont in item.get("continuation", []):
            if cont.get("definition"):
                hccs.extend(_extract_hccs_from_definition(cont["definition"]))

        if hccs:
            groups[variable] = hccs

    _CACHE[cache_key] = groups
    return groups


def _extract_hccs_from_definition(definition: str) -> list[str]:
    """Extract HCC numbers from a SAS-style definition string.

    Example: "if HHS_HCC019 = 1 then do; HHS_HCC019 = 0; G01 = 1; end;"
    Returns: ["019"]
    """
    import re

    hccs = []
    # Match patterns like HHS_HCC019, HHS_HCC035_1, etc.
    pattern = r"HHS_HCC(\d+(?:_\d+)?)"
    matches = re.findall(pattern, definition)
    hccs.extend(matches)
    return hccs


def load_model_exclusions(model_year: str = "2024") -> dict[str, set[str]]:
    """Load HCCs excluded from each model from table_12.parquet.

    Args:
        model_year: Model year (e.g., "2024")

    Returns:
        Dictionary mapping model name to set of excluded HCCs
        e.g., {"Adult": {"28", "64"}, "Child": {"22", "174"}, ...}
    """
    cache_key = f"exclusions_{model_year}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tables_dir = _get_tables_dir(model_year)
    exclusions: dict[str, set[str]] = {
        "Adult": set(),
        "Child": set(),
        "Infant": set(),
    }

    df = pl.read_parquet(tables_dir / "table_12.parquet")

    # Determine HCC column name (e.g., v07_hhs-hcc for 2024, v08_hhs-hcc for 2025)
    hcc_col = next((col for col in df.columns if col.endswith("_hhs-hcc")), "v07_hhs-hcc")

    for row in df.iter_rows(named=True):
        hcc = str(row[hcc_col]).strip()

        if str(row.get("payment_hccs_excluded_from_adult_model", "") or "").strip():
            exclusions["Adult"].add(hcc)
        if str(row.get("payment_hccs_excluded_from_child_model", "") or "").strip():
            exclusions["Child"].add(hcc)
        if str(row.get("payment_hccs_excluded_from_infant_model", "") or "").strip():
            exclusions["Infant"].add(hcc)

    _CACHE[cache_key] = exclusions
    return exclusions


def load_ndc_to_rxc(model_year: str = "2024") -> dict[str, list[str]]:
    """Load NDC to RXC mappings from table_10a.parquet.

    Args:
        model_year: Model year (e.g., "2024")

    Returns:
        Dictionary mapping NDC codes to list of RXCs
    """
    cache_key = f"ndc_to_rxc_{model_year}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tables_dir = _get_tables_dir(model_year)
    ndc_to_rxc: dict[str, list[str]] = {}

    df = pl.read_parquet(tables_dir / "table_10a.parquet")

    # Identify columns
    ndc_col = next((col for col in df.columns if "ndc" in col.lower()), "ndc")
    rxc_col = next((col for col in df.columns if "rxc" in col.lower()), "rxc")

    for row in df.iter_rows(named=True):
        ndc = str(row[ndc_col]).strip()
        rxc = str(row[rxc_col]).strip()

        if ndc and rxc:
            if ndc not in ndc_to_rxc:
                ndc_to_rxc[ndc] = []
            if rxc not in ndc_to_rxc[ndc]:
                ndc_to_rxc[ndc].append(rxc)

    _CACHE[cache_key] = ndc_to_rxc
    return ndc_to_rxc


def load_rxc_hierarchies(model_year: str = "2024") -> dict[str, list[str]]:
    """Load RXC hierarchies from table_11.parquet.

    Args:
        model_year: Model year (e.g., "2024")

    Returns:
        Dictionary mapping dominant RXC to list of RXCs it supersedes
    """
    cache_key = f"rxc_hierarchies_{model_year}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tables_dir = _get_tables_dir(model_year)
    hierarchies: dict[str, list[str]] = {}

    df = pl.read_parquet(tables_dir / "table_11.parquet")

    # Determine RXC column name (e.g., v07_rxc for 2024).
    # Some CMS releases use slightly different naming, so be flexible.
    rxc_candidates = [col for col in df.columns if "rxc" in col.lower()]
    if not rxc_candidates:
        # If we can't confidently identify an RXC column, return empty hierarchies
        # rather than raising a KeyError during row access.
        _CACHE[cache_key] = hierarchies
        return hierarchies
    rxc_col = rxc_candidates[0]

    # Determine zero column (e.g., rxcs_to_zero). Fall back to none if missing.
    zero_candidates = [col for col in df.columns if "zero" in col.lower()]
    zero_col = zero_candidates[0] if zero_candidates else None

    for row in df.iter_rows(named=True):
        rxc = str(row[rxc_col]).strip()
        zeros = str(row.get(zero_col, "") or "").strip() if zero_col else ""

        if zeros:
            # Parse comma-separated list, handling spaces
            superseded = [r.strip() for r in zeros.split(",") if r.strip()]
            if superseded:
                hierarchies[rxc] = superseded

    _CACHE[cache_key] = hierarchies
    return hierarchies


def clear_cache() -> None:
    """Clear the table cache."""
    _CACHE.clear()
