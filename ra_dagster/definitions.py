"""
Module: definitions.py
Description:
    Dagster repository definition.
    - Aggregates all assets, jobs, and resources.
    - Loads default configurations from YAML files.
    - Defines the top-level `Definitions` object for the Dagster daemon.

Usage:
    Loaded by Dagster to construct the execution graph.
"""
from pathlib import Path

import yaml
from dagster import Definitions, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource

from ra_dagster.assets.comparison import compare_runs
from ra_dagster.assets.comparison_dashboard import comparison_dashboard_html, comparison_dashboard_metrics
from ra_dagster.assets.dashboard import dashboard_html, dashboard_metrics
from ra_dagster.assets.dbt_assets import dbt_analytics_assets, dbt_project
from ra_dagster.assets.decomposition import decompose_runs
from ra_dagster.assets.scoring import score_members_aca
from ra_dagster.assets.visualizations import (
    comparison_visualizations,
    decomposition_visualizations,
    lag_trend_visualizations,
    scoring_visualizations,
)
from ra_dagster.assets.aca_reference_data import (
    aca_icd_to_cc_mapping,
    aca_hcc_hierarchies,
    aca_coefficients,
    aca_model_exclusions,
    aca_hcc_groups_adult,
    aca_hcc_groups_child,
    aca_ndc_to_rxc_mapping,
    aca_rxc_hierarchies,
    aca_hcc_labels,
    aca_rxc_labels,
)
from ra_dagster.assets.aca_scoring_detailed import (
    aca_member_input,
    aca_demographics,
    aca_raw_ccs,
    aca_hccs_post_hierarchy,
    aca_hccs_final,
    aca_rxcs,
    aca_scores,
)
from ra_dagster.resources.sqlalchemy_resource import db_resource

CONFIGS_DIR = Path(__file__).resolve().parent / "configs"

# Load default configs (used as job defaults when launching from the UI/CLI)
with open(CONFIGS_DIR / "decomposition" / "decomposition_example.yaml", encoding="utf-8") as f:
    default_decomp_config = yaml.safe_load(f)

with open(CONFIGS_DIR / "comparison" / "comparison_example.yaml", encoding="utf-8") as f:
    default_comp_config = yaml.safe_load(f)

with open(CONFIGS_DIR / "scoring" / "scoring_example.yaml", encoding="utf-8") as f:
    default_scoring_config = yaml.safe_load(f)

# Construct config for detailed scoring job by distributing the monolith config
# to the relevant assets that use the same ScoringConfig schema.
_scoring_ops_config = default_scoring_config["ops"]["score_members_aca"]["config"]
detailed_scoring_config = {
    "resources": default_scoring_config.get("resources", {}),
    "ops": {
        "aca_member_input": {"config": _scoring_ops_config},
        "aca_demographics": {"config": _scoring_ops_config},
        "aca_scores": {"config": _scoring_ops_config},
    },
}

with open(CONFIGS_DIR / "dashboard" / "dashboard_config.yaml", encoding="utf-8") as f:
    default_dashboard_config = yaml.safe_load(f)

with open(CONFIGS_DIR / "comparison" / "comparison_dashboard_config.yaml", encoding="utf-8") as f:
    default_comparison_dashboard_config = yaml.safe_load(f)

scoring_job = define_asset_job(
    name="scoring_job",
    selection=AssetSelection.assets(score_members_aca, scoring_visualizations) | AssetSelection.assets(dbt_analytics_assets),
    description="""
    # ACA Risk Scoring Job

    Calculates risk scores for all members in the intermediate table.

    **Steps:**
    1. Reads member data from `int_aca_risk_input`
    2. Applies HHS-HCC model logic
    3. Writes results to `dag_runs.risk_scores`
    4. Materializes dbt analytics marts
    """,
    tags={"team": "analytics", "priority": "high"},
    metadata={
        "owner": "Garth Mortensen",
        "docs": "https://github.com/garthmortensen/prism/ra_dagster",
    },
    config=default_scoring_config,
)

comparison_job = define_asset_job(
    name="comparison_job",
    selection=["compare_runs", "comparison_visualizations"],
    description="""
    # Run Comparison Job

    Compares two scoring runs and writes summary + member-level deltas.

    **Steps:**
    1. Reads scores from `dag_runs.risk_scores` for `run_id_a` and `run_id_b`
    2. Aligns members (intersection/union/etc.) and computes deltas
    3. Writes results to `analytics.run_comparison`
    """,
    tags={"team": "analytics", "priority": "high"},
    metadata={
        "owner": "Garth Mortensen",
        "docs": "https://github.com/garthmortensen/prism/ra_dagster",
    },
    config=default_comp_config,
)

decomposition_job = define_asset_job(
    name="decomposition_job",
    selection=["decompose_runs", "decomposition_visualizations"],
    description="""
    # Risk Decomposition Job

    Decomposes the difference between two runs into component contributions
    (e.g., model coefficients vs population mix) across multiple scenarios.

    **Steps:**
    1. Reads scenario run scores from `dag_runs.risk_scores`
    2. Applies the configured decomposition method (marginal)
    3. Writes results to `analytics.decomposition_definitions`
       + `analytics.decomposition_scenarios`
    """,
    tags={"team": "analytics", "priority": "high"},
    metadata={
        "owner": "Garth Mortensen",
        "docs": "https://github.com/garthmortensen/prism/ra_dagster",
    },
    config=default_decomp_config,
)

dashboard_job = define_asset_job(
    name="dashboard_job",
    selection=["dashboard_metrics", "dashboard_html"],
    description="""
    # Population Dashboard Job

    Calculates population metrics (demographics, risk scores) for a specific run
    and generates an HTML dashboard.

    **Steps:**
    1. Reads risk scores and member data for `run_id`
    2. Computes aggregate metrics (Age, Gender, Metal Level)
    3. Generates an HTML report
    """,
    tags={"team": "analytics", "priority": "medium"},
    metadata={
        "owner": "Garth Mortensen",
        "docs": "https://github.com/garthmortensen/prism/ra_dagster",
    },
    config=default_dashboard_config,
)

comparison_dashboard_job = define_asset_job(
    name="comparison_dashboard_job",
    selection=["comparison_dashboard_metrics", "comparison_dashboard_html"],
    description="""
    # Comparison Dashboard Job

    Calculates comparison metrics for a specific comparison batch and generates an HTML dashboard.

    **Steps:**
    1. Reads comparison data for `batch_id`
    2. Computes aggregate metrics (Match Status, Score Diff)
    3. Generates an HTML report
    """,
    tags={"team": "analytics", "priority": "medium"},
    metadata={
        "owner": "Garth Mortensen",
        "docs": "https://github.com/garthmortensen/prism/ra_dagster",
    },
    config=default_comparison_dashboard_config,
)

# Reference data assets (materialized once per model year)
aca_reference_data_assets = [
    aca_icd_to_cc_mapping,
    aca_hcc_hierarchies,
    aca_coefficients,
    aca_model_exclusions,
    aca_hcc_groups_adult,
    aca_hcc_groups_child,
    aca_ndc_to_rxc_mapping,
    aca_rxc_hierarchies,
    aca_hcc_labels,
    aca_rxc_labels,
]

# Detailed scoring pipeline assets
aca_scoring_detailed_assets = [
    aca_member_input,
    aca_demographics,
    aca_raw_ccs,
    aca_hccs_post_hierarchy,
    aca_hccs_final,
    aca_rxcs,
    aca_scores,
]

aca_scoring_detailed_job = define_asset_job(
    name="aca_scoring_detailed_job",
    selection=AssetSelection.groups("aca_reference_data", "aca_scoring_detailed"),
    description="""
    # ACA Detailed Scoring Job

    A granular, step-by-step risk scoring pipeline with full DAG visibility.
    Each step is a separate asset that can be inspected and debugged independently.

    **Steps:**
    1. **aca_member_input**: Load member data from database
    2. **aca_demographics**: Calculate age, model type, demographic coefficients
    3. **aca_raw_ccs**: Map ICD-10 diagnoses to Condition Categories
    4. **aca_hccs_post_hierarchy**: Apply HCC hierarchy rules
    5. **aca_hccs_final**: Apply model exclusions and HCC groupings
    6. **aca_rxcs**: Map NDCs to RXCs and apply RXC hierarchies
    7. **aca_scores**: Calculate final risk scores

    **Reference Data Assets (cached per model year):**
    - aca_icd_to_cc_mapping (Table 3)
    - aca_hcc_hierarchies (Table 4)
    - aca_coefficients (Table 9)
    - aca_model_exclusions (Table 12)
    - aca_hcc_groups_adult/child (Tables 6/7)
    - aca_ndc_to_rxc_mapping (Table 10a)
    - aca_rxc_hierarchies (Table 11)
    """,
    tags={"team": "analytics", "priority": "high"},
    metadata={
        "owner": "Garth Mortensen",
        "docs": "https://github.com/garthmortensen/prism/ra_dagster",
    },
    config=detailed_scoring_config,
)


definitions = Definitions(
    assets=[
        score_members_aca,
        compare_runs,
        decompose_runs,
        scoring_visualizations,
        comparison_visualizations,
        decomposition_visualizations,
        lag_trend_visualizations,
        dashboard_metrics,
        dashboard_html,
        comparison_dashboard_metrics,
        comparison_dashboard_html,
        dbt_analytics_assets,
        # ACA Reference Data assets (cached per model year)
        *aca_reference_data_assets,
        # ACA Detailed Scoring assets (granular DAG)
        *aca_scoring_detailed_assets,
    ],
    resources={
        "database": db_resource,
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
    jobs=[
        scoring_job,
        comparison_job,
        decomposition_job,
        dashboard_job,
        comparison_dashboard_job,
        aca_scoring_detailed_job,
    ],
)
