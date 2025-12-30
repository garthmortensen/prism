from __future__ import annotations

from dagster import Definitions, define_asset_job

from ra_dagster.assets.comparison import compare_runs
from ra_dagster.assets.decomposition import decompose_runs
from ra_dagster.assets.scoring import score_members_aca
from ra_dagster.resources.duckdb_resource import DuckDBResource

scoring_job = define_asset_job(
    name="scoring_job",
    selection=["score_members_aca"],
)

comparison_job = define_asset_job(
    name="comparison_job",
    selection=["compare_runs"],
)

decomposition_job = define_asset_job(
    name="decomposition_job",
    selection=["decompose_runs"],
)


definitions = Definitions(
    assets=[score_members_aca, compare_runs, decompose_runs],
    resources={
        "duckdb": DuckDBResource(),
    },
    jobs=[scoring_job, comparison_job, decomposition_job],
)
