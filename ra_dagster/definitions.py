import yaml
from dagster import Definitions, define_asset_job

from ra_dagster.assets.comparison import compare_runs
from ra_dagster.assets.decomposition import decompose_runs
from ra_dagster.assets.scoring import score_members_aca
from ra_dagster.resources.duckdb_resource import DuckDBResource

# Load default decomposition config
with open("ra_dagster/configs/decomposition_example.yaml") as f:
    default_decomp_config = yaml.safe_load(f)

# Load default comparison config
with open("ra_dagster/configs/comparison_example.yaml") as f:
    default_comp_config = yaml.safe_load(f)

# Load default scoring config
with open("ra_dagster/configs/scoring_example.yaml") as f:
    default_scoring_config = yaml.safe_load(f)

scoring_job = define_asset_job(
    name="scoring_job",
    selection=["score_members_aca"],
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
    selection=["compare_runs"],
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
    selection=["decompose_runs"],
    description="""
    # Risk Decomposition Job

    Decomposes the difference between two runs into component contributions
    (e.g., model coefficients vs population mix) across multiple scenarios.

    **Steps:**
    1. Reads scenario run scores from `main_runs.risk_scores`
    2. Applies the configured decomposition method (e.g., sequential)
    3. Writes results to `main_analytics.decomposition_definitions` + `main_analytics.decomposition_scenarios`
    """,
    tags={"team": "analytics", "priority": "high"},
    metadata={
        "owner": "Garth Mortensen",
        "docs": "https://github.com/garthmortensen/prism/ra_dagster",
    },
    config=default_decomp_config,
)


definitions = Definitions(
    assets=[score_members_aca, compare_runs, decompose_runs],
    resources={
        "duckdb": DuckDBResource(),
    },
    jobs=[scoring_job, comparison_job, decomposition_job],
)
