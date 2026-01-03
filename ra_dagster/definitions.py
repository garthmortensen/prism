from pathlib import Path

import yaml
from dagster import Definitions, define_asset_job

from ra_dagster.assets.comparison import compare_runs
from ra_dagster.assets.dashboard import dashboard_html, dashboard_metrics
from ra_dagster.assets.decomposition import decompose_runs
from ra_dagster.assets.scoring import score_members_aca
from ra_dagster.assets.visualizations import (
    comparison_visualizations,
    decomposition_visualizations,
    lag_trend_visualizations,
    scoring_visualizations,
)
from ra_dagster.resources.duckdb_resource import DuckDBResource

CONFIGS_DIR = Path(__file__).resolve().parent / "configs"

# Load default configs (used as job defaults when launching from the UI/CLI)
with open(CONFIGS_DIR / "decomposition" / "decomposition_example.yaml", encoding="utf-8") as f:
    default_decomp_config = yaml.safe_load(f)

with open(CONFIGS_DIR / "comparison" / "comparison_example.yaml", encoding="utf-8") as f:
    default_comp_config = yaml.safe_load(f)

with open(CONFIGS_DIR / "scoring" / "scoring_example.yaml", encoding="utf-8") as f:
    default_scoring_config = yaml.safe_load(f)

with open(CONFIGS_DIR / "dashboard" / "dashboard_config.yaml", encoding="utf-8") as f:
    default_dashboard_config = yaml.safe_load(f)

scoring_job = define_asset_job(
    name="scoring_job",
    selection=["score_members_aca", "scoring_visualizations"],
    description="""
    # ACA Risk Scoring Job

    Calculates risk scores for all members in the intermediate table.

    **Steps:**
    1. Reads member data from `int_aca_risk_input`
    2. Applies HHS-HCC model logic
    3. Writes results to `main_runs.risk_scores`
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
    1. Reads scores from `main_runs.risk_scores` for `run_id_a` and `run_id_b`
    2. Aligns members (intersection/union/etc.) and computes deltas
    3. Writes results to `main_analytics.run_comparison`
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
    1. Reads scenario run scores from `main_runs.risk_scores`
    2. Applies the configured decomposition method (marginal)
    3. Writes results to `main_analytics.decomposition_definitions`
       + `main_analytics.decomposition_scenarios`
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
    ],
    resources={
        "duckdb": DuckDBResource(),
    },
    jobs=[scoring_job, comparison_job, decomposition_job, dashboard_job],
)
