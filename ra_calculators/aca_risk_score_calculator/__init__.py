"""ACA HHS-HCC Risk Score Calculator.

Implements the CMS HHS-HCC risk adjustment model for ACA markets.
Loads official DIY tables from diy_tables/cy202*_diy_tables/.
"""

from ra_calculators.aca_risk_score_calculator.calculator import ACACalculator
from ra_calculators.aca_risk_score_calculator.models import MemberInput, ScoreComponent, ScoreOutput
from ra_calculators.aca_risk_score_calculator.scoring_steps import (
    apply_hcc_groupings,
    apply_hcc_hierarchies,
    apply_rxc_hierarchies,
    calculate_age,
    calculate_edf,
    calculate_hcc_scores,
    calculate_rxc_scores,
    calculate_total_score,
    exclude_model_hccs,
    find_source_icds_for_hcc,
    find_source_ndcs_for_rxc,
    format_hcc_variable_name,
    format_rxc_variable_name,
    get_coefficient,
    get_demographic_variable,
    get_model_type,
    map_diagnoses_to_ccs,
    map_ndcs_to_rxcs,
)

__all__ = [
    # Main calculator class
    "ACACalculator",
    # Models
    "MemberInput",
    "ScoreComponent",
    "ScoreOutput",
    # Pure functions for Dagster asset-based orchestration
    "apply_hcc_groupings",
    "apply_hcc_hierarchies",
    "apply_rxc_hierarchies",
    "calculate_age",
    "calculate_edf",
    "calculate_hcc_scores",
    "calculate_rxc_scores",
    "calculate_total_score",
    "exclude_model_hccs",
    "find_source_icds_for_hcc",
    "find_source_ndcs_for_rxc",
    "format_hcc_variable_name",
    "format_rxc_variable_name",
    "get_coefficient",
    "get_demographic_variable",
    "get_model_type",
    "map_diagnoses_to_ccs",
    "map_ndcs_to_rxcs",
]
