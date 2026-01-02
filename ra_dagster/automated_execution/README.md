# Automated Execution

This directory contains scripts to automate the execution of Dagster jobs for Risk Adjustment analysis.

## `launch_analyses.py`

This script launches Dagster runs for all YAML configuration files found in `ra_dagster/configs/`. It supports batch execution for scoring, decomposition, and comparison jobs.

### Usage

Run the script from the project root directory:

```bash
# Run all scoring configs
python ra_dagster/automated_execution/launch_analyses.py scoring

# Run all decomposition configs
python ra_dagster/automated_execution/launch_analyses.py decomposition

# Run all comparison configs
python ra_dagster/automated_execution/launch_analyses.py comparison

# Run ALL batches in sequence (scoring -> decomposition -> comparison)
# Note: Ensure Run IDs are updated in configs if dependencies exist.
python ra_dagster/automated_execution/launch_analyses.py all
```

### Prerequisites (DuckDB + dbt)

The scoring configs reference dbt-created views like `main_raw.raw_claims_2024` (DuckDB/dbt may display these as `main_main_raw.*`). Make sure seeds + models are built before launching runs:

```bash
# One-time: create schemas (if needed)
make db-bootstrap

# Load seed tables and build views/models
cd ra_dbt
uv run dbt seed
uv run dbt run
```

### Viewing Runs In The Dagster UI

`launch_analyses.py` launches runs into the Dagster instance at `DAGSTER_HOME`. If `DAGSTER_HOME` is not set, the script defaults to `./.dagster_home` (the same instance directory used by `make dagster`).

To see those runs in the UI, start Dagster against the same instance directory:

```bash
export DAGSTER_HOME="$PWD/.dagster_home"
uv run dagster dev -m ra_dagster.definitions
```

### Features

- **Batch Processing**: Iterates through all `.yaml` files in the corresponding `ra_dagster/configs/<subfolder>`.
- **File Skipping**: Automatically skips files that:
  - Start with `.` (hidden) or `_`
  - End with `.bak` or `.disabled`
  - Contain `example`, `ignore`, `skip`, or `xxx` in the filename
- **Environment Setup**: Uses `DAGSTER_HOME` if set; otherwise defaults to `./.dagster_home` to match `make dagster`.
- **Output**: Prints the Run ID for each successful launch.

### Directory Structure

The script expects the following configuration structure:

- `ra_dagster/configs/scoring/` -> `scoring_job`
- `ra_dagster/configs/decomposition/` -> `decomposition_job`
- `ra_dagster/configs/comparison/` -> `comparison_job`
