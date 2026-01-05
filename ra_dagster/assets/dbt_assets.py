from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# Point to the dbt project root
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "ra_dbt"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_analytics_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
