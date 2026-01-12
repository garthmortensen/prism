"""
Asset: dbt_assets.py
Description:
    Wraps the dbt project as a Dagster asset definition.
    - Loads the dbt project manifest.
    - Exposes dbt models as Dagster assets for orchestration.

Usage:
    Imported into definitions.py to register the dbt graph.
"""
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject, DagsterDbtTranslator
from dagster import AssetKey
from typing import Mapping, Any

# Point to the dbt project root
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "ra_dbt"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

class PrismDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        node_type = dbt_resource_props.get("resource_type")
        name = dbt_resource_props.get("name")
        source_name = dbt_resource_props.get("source_name")

        if node_type == "source" and source_name == "dagster_analytics" and name == "run_comparison":
             return AssetKey("compare_runs")
        
        return super().get_asset_key(dbt_resource_props)

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=PrismDbtTranslator()
)
def dbt_analytics_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
