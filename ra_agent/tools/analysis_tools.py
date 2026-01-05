import os
import yaml
import uuid
import subprocess
import tempfile
from langchain_core.tools import tool

@tool
def compare_two_runs(run_id_a: str, run_id_b: str) -> str:
    """
    Compare two scoring runs and compute deltas.
    Triggers a Dagster job to perform the comparison and update analytics tables.
    
    Args:
        run_id_a: The first run UUID (baseline)
        run_id_b: The second run UUID (comparison)
    """
    
    # Create a temporary config file
    config = {
        "ops": {
            "compare_runs": {
                "config": {
                    "run_id_a": run_id_a,
                    "run_id_b": run_id_b
                }
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        config_path = f.name
        
    try:
        # Execute the Dagster job
        # We assume we are in the project root or can find the definitions file
        # Using 'uv run' to ensure we use the project environment
        cmd = [
            "uv", "run", "dagster", "job", "execute",
            "-f", "ra_dagster/definitions.py",
            "-j", "comparison_job",
            "-c", config_path
        ]
        
        print(f"Executing comparison job for {run_id_a} vs {run_id_b}...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            return f"Error executing comparison job:\n{result.stderr}"
            
        return f"Comparison job completed successfully. You can now use get_comparison_by_dimension('{run_id_a}', '{run_id_b}') to view results."
        
    except Exception as e:
        return f"Error triggering comparison: {str(e)}"
        
    finally:
        # Clean up config file
        if os.path.exists(config_path):
            os.remove(config_path)
