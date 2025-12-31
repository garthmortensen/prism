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
    config={
        "ops": {
            "score_members_aca": {
                "config": {
                    "model_year": "2024",
                    "run_description": "ACA scoring run (Manual Trigger)",
                    "invalid_gender": "skip",
                }
            }
        }
    },
)

comparison_job = define_asset_job(
    name="comparison_job",
    selection=["compare_runs"],
    config=default_comp_config,
)

decomposition_job = define_asset_job(
    name="decomposition_job",
    selection=["decompose_runs"],
    config=default_decomp_config,
)


definitions = Definitions(
    assets=[score_members_aca, compare_runs, decompose_runs],
    resources={
        "duckdb": DuckDBResource(),
    },
    jobs=[scoring_job, comparison_job, decomposition_job],
)
