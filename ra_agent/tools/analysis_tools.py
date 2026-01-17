import os
import yaml
import uuid
import subprocess
import tempfile
from pathlib import Path
from typing import Literal
from enum import Enum

from pydantic import BaseModel, Field
from langchain_core.tools import tool
from ra_agent.db import resolve_run_id

# Global variable to store last execution error for debugging
_last_execution_error = {"stdout": "", "stderr": "", "returncode": 0, "cmd": ""}

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

class ScoringRunInput(BaseModel):
    """Input for configure_scoring_run tool."""
    diy_model_year: Literal[2021, 2022, 2023, 2024, 2025] = Field(
        2024,
        description="DIY model year (2021-2025)"
    )
    run_description: str = Field(
        ...,
        description="Description for the run"
    )
    member_age_basis_year: str | None = Field(
        None,
        description="Year for age calculation"
    )
    claims_view: str | None = Field(
        None,
        description="Override claims table"
    )
    enrollments_view: str | None = Field(
        None,
        description="Override enrollments table"
    )
    members_view: str | None = Field(
        None,
        description="Override members table"
    )
    invalid_gender: Literal["skip", "coerce", "error"] = Field(
        "skip",
        description="Gender handling: skip, coerce, or error"
    )
    coerce_gender: Literal["M", "F"] | None = Field(
        None,
        description="Target gender for coercion"
    )
    group_id: int | None = Field(
        None,
        description="Group runs together"
    )
    group_description: str | None = Field(
        None,
        description="Batch description"
    )
    save_config_path: str | None = Field(
        None,
        description="Path to save config file"
    )


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
    run_description: str | None = Field(
        None, 
        description="Description for the comparison run."
    )
    metric: Literal["mean", "sum"] | None = Field(
        "mean", 
        description="Metric to compare (mean or sum)."
    )
    population_mode: Literal["intersection", "union", "a_only", "b_only"] | None = Field(
        "intersection", 
        description="How to align the populations (intersection, union, etc)."
    )


@tool(args_schema=ComparisonInput)
def compare_two_runs(
    run_id_a: str, 
    run_id_b: str,
    run_description: str | None = None,
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
            },
            "resources": {
                "database": {
                    "config": "dev"
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
        # Using 'uv run' to ensure we use the project environment
        cmd = [
            "uv", "run", "dagster", "job", "launch",
            "-m", "ra_dagster.definitions",
            "-j", "comparison_job",
            "-c", config_path
        ]
        
        # Set environment variables for Dagster
        env = os.environ.copy()
        if "DAGSTER_HOME" not in env:
            # Use the project's .dagster_home directory (matches launch_analyses.py)
            project_root = Path.cwd()
            dagster_home = project_root / ".dagster_home"
            dagster_home.mkdir(parents=True, exist_ok=True)
            
            # Ensure dagster.yaml exists
            dagster_yaml = dagster_home / "dagster.yaml"
            if not dagster_yaml.exists():
                with open(dagster_yaml, "w") as f:
                    f.write("telemetry:\n  enabled: false\n")
            
            env["DAGSTER_HOME"] = str(dagster_home)
        
        print(f"Executing comparison job for {run_ref_a} vs {run_ref_b}...")
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        
        if result.returncode != 0:
            return f"Error executing comparison job:\n{result.stderr}"
            
        return f"Comparison job completed successfully. You can now use get_comparison_by_dimension('{run_id_a}', '{run_id_b}') to view results."
        
    except Exception as e:
        return f"Error triggering comparison: {str(e)}"
        
    finally:
        # Clean up config file
        if config_path and os.path.exists(config_path):
            os.remove(config_path)


@tool(args_schema=ScoringRunInput)
def configure_scoring_run(
    diy_model_year: int,
    run_description: str,
    member_age_basis_year: str | None = None,
    claims_view: str | None = None,
    enrollments_view: str | None = None,
    members_view: str | None = None,
    invalid_gender: str = "skip",
    coerce_gender: str | None = None,
    group_id: int | None = None,
    group_description: str | None = None,
    save_config_path: str | None = None
) -> str:
    """
    Generate YAML configuration for a risk scoring run.
    Returns config content and optionally saves to file.
    """
    try:
        # Build config structure matching Dagster's expected format
        # Set defaults for views and member_age_basis_year
        if claims_view is None:
            claims_view = f"main_raw.raw_claims_{diy_model_year}"
        if enrollments_view is None:
            enrollments_view = f"main_raw.raw_enrollments_{diy_model_year}"
        if members_view is None:
            members_view = f"main_raw.raw_members_{diy_model_year}"
        if member_age_basis_year is None:
            member_age_basis_year = str(diy_model_year)
        
        config = {
            "ops": {
                "dagster_runs_outputs__risk_scores": {
                    "config": {
                        "claims_view": claims_view,
                        "enrollments_view": enrollments_view,
                        "members_view": members_view,
                        "invalid_gender": invalid_gender,
                        "diy_model_year": str(diy_model_year),
                        "member_age_basis_year": member_age_basis_year,
                        "run_description": run_description
                    }
                }
            },
            "resources": {
                "database": {
                    "config": "dev"
                }
            }
        }
        
        # Add optional fields only if explicitly provided
        op_config = config["ops"]["dagster_runs_outputs__risk_scores"]["config"]
        if coerce_gender:
            op_config["coerce_gender"] = coerce_gender
        if group_id is not None:
            op_config["group_id"] = group_id
        if group_description:
            op_config["group_description"] = group_description
        
        # Convert to YAML
        yaml_content = yaml.dump(config, sort_keys=False, default_flow_style=False)
        
        # Save if path provided
        saved_path = None
        if save_config_path:
            from pathlib import Path
            config_path = Path(save_config_path)
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                f.write(yaml_content)
            saved_path = str(config_path.resolve())
        
        result = f"Generated config:\n\n{yaml_content}"
        if saved_path:
            result += f"\nSaved to: {saved_path}"
        
        return result
        
    except Exception as e:
        return f"Error generating config: {str(e)}"


@tool
def execute_scoring_run(
    config_yaml: str | None = None,
    config_path: str | None = None
) -> str:
    """
    Execute a Dagster scoring job.
    Provide either config_yaml (string) or config_path (file path).
    """
    global _last_execution_error
    
    try:
        temp_config_path = None
        
        # Determine config file
        if config_path:
            if not os.path.exists(config_path):
                return f"Config file not found: {config_path}"
            final_config_path = config_path
        elif config_yaml:
            # Create temp file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(config_yaml)
                temp_config_path = f.name
                final_config_path = temp_config_path
        else:
            return "Provide either config_yaml or config_path"
        
        # Execute Dagster job
        cmd = [
            "uv", "run", "dagster", "job", "launch",
            "-m", "ra_dagster.definitions",
            "-j", "scoring_job",
            "-c", final_config_path
        ]
        
        # Set environment variables for Dagster
        env = os.environ.copy()
        if "DAGSTER_HOME" not in env:
            # Use the project's .dagster_home directory (matches launch_analyses.py)
            project_root = Path.cwd()
            dagster_home = project_root / ".dagster_home"
            dagster_home.mkdir(parents=True, exist_ok=True)
            
            # Ensure dagster.yaml exists
            dagster_yaml = dagster_home / "dagster.yaml"
            if not dagster_yaml.exists():
                with open(dagster_yaml, "w") as f:
                    f.write("telemetry:\n  enabled: false\n")
            
            env["DAGSTER_HOME"] = str(dagster_home)
        
        print(f"Executing scoring job...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, env=env)
        
        # Store execution details for error inspection
        _last_execution_error = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
            "cmd": " ".join(cmd)
        }
        
        if result.returncode != 0:
            # Return concise error with hint to use get_last_error for details
            error_preview = result.stderr[:500] if result.stderr else "No error details"
            return (
                f"Job execution failed (exit code {result.returncode}).\n\n"
                f"Error preview:\n{error_preview}\n\n"
                f"Use get_last_execution_error() to see full error details."
            )
        
        # Extract run info
        output = result.stdout
        run_info = "Scoring job completed successfully."
        
        for line in output.splitlines():
            if "Run Ref:" in line or "run_ref" in line.lower():
                run_info += f"\n{line.strip()}"
        
        run_info += "\n\nUse list_runs() to see details."
        
        return run_info
        
    except subprocess.TimeoutExpired:
        return "Job timed out after 10 minutes."
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if temp_config_path and os.path.exists(temp_config_path):
            os.remove(temp_config_path)


@tool
def get_last_execution_error() -> str:
    """
    Get detailed error information from the last job execution.
    Shows full stdout, stderr, exit code, and command used.
    Useful for debugging failed scoring runs.
    """
    global _last_execution_error
    
    if not _last_execution_error.get("cmd"):
        return "No execution history available. Run execute_scoring_run first."
    
    details = f"""Last Execution Details:
    
Command: {_last_execution_error['cmd']}
Exit Code: {_last_execution_error['returncode']}

=== STDOUT ===
{_last_execution_error['stdout'] or '(empty)'}

=== STDERR ===
{_last_execution_error['stderr'] or '(empty)'}
"""
    
    return details
