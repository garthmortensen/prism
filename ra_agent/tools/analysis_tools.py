import os
import yaml
import uuid
import subprocess
import tempfile
from typing import Optional, Literal
from enum import Enum

from pydantic import BaseModel, Field
from langchain_core.tools import tool
from ra_agent.db import resolve_run_id

# -----------------------------------------------------------------------------
# Enums
# -----------------------------------------------------------------------------

class ScoringModels(str, Enum):
    ACA = "ACA"
    MEDICARE = "Medicare"
    MEDICAID = "Medicaid"


class RiskScoreTypes(str, Enum):
    CONCURRENT = "Concurrent"
    PROSPECTIVE = "Prospective"


class Markets(str, Enum):
    INDIVIDUAL = "Individual"
    SMALL_GROUP = "Small Group"


# -----------------------------------------------------------------------------
# Input Models
# -----------------------------------------------------------------------------

class ComparisonInput(BaseModel):
    """Input for compare_two_runs tool."""
    run_id_a: str = Field(
        ..., 
        description="The first run UUID OR run_ref (baseline). e.g. '00ebabaf-c761-4e88-a1a1-a5fe6d7b0f1c'"
    )
    run_id_b: str = Field(
        ..., 
        description="The second run UUID OR run_ref (comparison). e.g. '01d45526-3798-48f6-88e7-7699baccb287'"
    )
    run_description: Optional[str] = Field(
        None, 
        description="Description for the comparison run."
    )
    metric: Optional[Literal["mean", "sum"]] = Field(
        "mean", 
        description="Metric to compare (mean or sum)."
    )
    population_mode: Optional[Literal["intersection", "union", "a_only", "b_only"]] = Field(
        "intersection", 
        description="How to align the populations (intersection, union, etc)."
    )


@tool(args_schema=ComparisonInput)
def compare_two_runs(
    run_id_a: str, 
    run_id_b: str,
    run_description: Optional[str] = None,
    metric: str = "mean",
    population_mode: str = "intersection"
) -> str:
    """
    Compare two scoring runs and compute deltas.
    Triggers a Dagster job to perform the comparison and update analytics tables.
    """
    try:
        # Resolve to canonical run_ref (or UUID string)
        run_ref_a = resolve_run_id(run_id_a)
        run_ref_b = resolve_run_id(run_id_b)
        
        # Create a temporary config file following Dagster structure
        config = {
            "ops": {
                "compare_runs": {
                    "config": {
                        "run_ref_a": run_ref_a,
                        "run_ref_b": run_ref_b,
                        "metric": metric,
                        "population_mode": population_mode
                    }
                }
            }
        }
        
        if run_description:
            config["ops"]["compare_runs"]["config"]["run_description"] = run_description
        
        config_path = None
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_path = f.name
            
        # Execute the Dagster job
        # We assume we are in the project root or can find the definitions file
        # Using 'uv run' to ensure we use the project environment
        cmd = [
            "uv", "run", "dagster", "job", "execute",
            "-f", "ra_dagster/definitions.py",
            "-j", "comparison_job",
            "-c", config_path
        ]
        
        print(f"Executing comparison job for {run_ref_a} vs {run_ref_b}...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            return f"Error executing comparison job:\n{result.stderr}"
            
        return f"Comparison job completed successfully. You can now use get_comparison_by_dimension('{run_id_a}', '{run_id_b}') to view results."
        
    except Exception as e:
        return f"Error triggering comparison: {str(e)}"
        
    finally:
        # Clean up config file
        if config_path and os.path.exists(config_path):
            os.remove(config_path)
