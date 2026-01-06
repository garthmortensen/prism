# Automated Execution

This directory contains scripts to automate the execution of Dagster jobs for Risk Adjustment analysis.

Risk Adjustment has an annual cycle, which materializes itself as repetively executed jobs. As year-end approaches, certain executions/reports become increasingly important. With dagster, such jobs can be easily setup and scheduled. 

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

### Finding results

Past hour results via:

```sql
SELECT 
    run_id, 
    run_description, 
    status, 
    created_at, 
    trigger_source
FROM main_runs.run_registry
WHERE created_at > now() - INTERVAL 30 MINUTE
  AND analysis_type = 'scoring'
ORDER BY created_at DESC;
```

### Viewing Runs In The Dagster UI

`launch_analyses.py` launches runs into the Dagster instance at `DAGSTER_HOME`. If `DAGSTER_HOME` is not set, the script defaults to `./.dagster_home` (the same instance directory used by `make dagster`).


## `permutation.py`

This tool automatically generates exhaustive configuration permutations for testing purposes. It inspects the configuration schemas (`ra_dagster/config_schemas.py`) and creates a specific configuration file for every possible combination of enumerated options (e.g. Model Years, Calculation modes).

### Output
The generated YAML files are written to: `ra_dagster/configs/permutations/`

### Usage
```bash
python ra_dagster/automated_execution/permutation.py
```

### Generation Logic
- **Scoring**: Generates a permutation for every combination of `ModelYearOption` and `InvalidGenderOption`.
    - *Exclusion Rule*: Configurations where `invalid_gender="error"` are skipped to prevent intentional pipeline failures during batch testing.
- **Comparison**: Generates permutations for all `MetricType` (mean/sum) and `PopulationMode` (intersection/union/etc) options.
- **Naming**: Files are named descriptively based on their parameters (e.g., `score_runs__diy_model_year-2024__invalid_gender-random.yaml`).

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
