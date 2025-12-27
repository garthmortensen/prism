"""HCC hierarchy application logic.

The HHS-HCC model applies hierarchies to avoid double-counting related conditions.
When a more severe condition is present, less severe related conditions are
"zeroed out" (removed from the score).

Example: Diabetes hierarchy
    - HCC 19 (Acute Complications) supersedes HCC 20, 21
    - HCC 20 (Chronic Complications) supersedes HCC 21
    - If member has HCC 19 + HCC 21, only HCC 19 counts
"""

from ra_calculators.aca_risk_calculator.table_loader import load_hierarchies


def apply_hierarchies(
    cc_set: set[str],
    model_year: str = "2024",
) -> set[str]:
    """Apply HCC hierarchies to remove superseded conditions.

    Args:
        cc_set: Set of condition categories before hierarchy application
        model_year: Model year for hierarchy rules

    Returns:
        Set of HCCs after removing superseded conditions
    """
    hierarchies = load_hierarchies(model_year)
    hcc_set = cc_set.copy()

    for dominant, superseded_list in hierarchies.items():
        if dominant in hcc_set:
            for superseded in superseded_list:
                hcc_set.discard(superseded)

    return hcc_set
