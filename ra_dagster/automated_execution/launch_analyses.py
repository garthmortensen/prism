import glob
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Ensure DAGSTER_HOME is set and points to a real directory.
# Match `make dagster`, which uses: $PWD/.dagster_home
raw_dagster_home = os.environ.get("DAGSTER_HOME")
if raw_dagster_home:
    dagster_home_dir = Path(raw_dagster_home)
    if not dagster_home_dir.is_absolute():
        dagster_home_dir = PROJECT_ROOT / dagster_home_dir
    dagster_home_source = "env"
else:
    dagster_home_dir = PROJECT_ROOT / ".dagster_home"
    dagster_home_source = "default"

dagster_home_dir.mkdir(parents=True, exist_ok=True)
os.environ["DAGSTER_HOME"] = str(dagster_home_dir)
print(f"[SETUP] DAGSTER_HOME ({dagster_home_source}): {dagster_home_dir}")

# Ensure dagster.yaml exists (avoids warning + disables telemetry by default)
dagster_yaml_path = dagster_home_dir / "dagster.yaml"
if not dagster_yaml_path.exists():
    with open(dagster_yaml_path, "w", encoding="utf-8") as f:
        f.write("telemetry:\n  enabled: false\n")
    print(f"[SETUP] Created {dagster_yaml_path}")

# Configuration
# Dagster CLI needs to load a module that exposes Definitions/Repository/etc.
MODULE_NAME = "ra_dagster.definitions"
SCORING_JOB = "scoring_job"  # Ensure this job exists in your Definitions
DECOMP_JOB = "decomposition_job"  # Ensure this job exists in your Definitions
COMPARE_JOB = "comparison_job"  # Ensure this job exists in your Definitions

# Base config directory (relative to repo root)
CONFIG_BASE_DIR = PROJECT_ROOT / "ra_dagster" / "configs"


def launch_run(job_name, config_path):
    """Launches a Dagster run via CLI."""
    print(f"[LAUNCH] {job_name} with {os.path.basename(config_path)}...")

    cmd = ["dagster", "job", "launch", "-m", MODULE_NAME, "-j", job_name, "-c", config_path]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        # Extract Run ID from stdout (usually "Launched run <RUN_ID>")
        for line in result.stdout.splitlines():
            if "Launched run" in line:
                print(f"   [SUCCESS] {line.strip()}")
    else:
        print(f"   [FAILED] {result.stderr}")


def run_batch(folder_name, job_name):
    """Runs all YAML configs in a specific subfolder."""
    folder_path = CONFIG_BASE_DIR / folder_name

    # glob.glob("*.yaml") AUTOMATICALLY ignores files starting with '.' (hidden files)
    # But we'll check explicitly just to be safe and cover other cases
    files = sorted(glob.glob(str(folder_path / "*.yaml")))

    if not files:
        print(f"No config files found in '{folder_path}'")
        return

    print(f"\n--- Starting Batch: {folder_name.upper()} ({len(files)} files) ---")
    for config_file in files:
        filename = os.path.basename(config_file)

        # Skip files starting with . or _ or ending with .bak or .disabled
        # or containing certain keywords
        if (
            filename.startswith(".")
            or filename.startswith("_")
            or filename.endswith(".bak")
            or filename.endswith(".disabled")
            or "example" in filename.lower()
            or "xxx" in filename.lower()
            or "skip" in filename.lower()
            or "ignore" in filename.lower()
        ):
            print(f"   [SKIP] {filename}")
            continue

        launch_run(job_name, config_file)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage: python ra_dagster/automated_execution/launch_analyses.py "
            "[scoring|decomposition|comparison|all]"
        )
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "scoring":
        run_batch("scoring", SCORING_JOB)
        print("\n[INFO] DONE. Now copy the Run IDs into your decomposition/comparison YAMLs.")

    elif mode == "decomposition":
        run_batch("decomposition", DECOMP_JOB)

    elif mode == "comparison":
        run_batch("comparison", COMPARE_JOB)

    elif mode == "all":
        print("Running ALL batches (Note: Ensure Run IDs are already updated!)")
        run_batch("scoring", SCORING_JOB)
        run_batch("decomposition", DECOMP_JOB)
        run_batch("comparison", COMPARE_JOB)

    else:
        print(f"Unknown mode: {mode}")
