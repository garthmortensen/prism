# ra_dbt

This dbt project transforms raw enrollment and claims data into the input format required for the ACA Risk Calculator.

## Models

- **Staging**: Cleans raw data from seeds/sources.
- **Intermediate**: Aggregates diagnoses and NDCs per member.
- **Intermediate (final)**: `int_aca_risk_input` joins everything into a single relation ready for the Python calculator (Dagster reads from here).

## Running

1.  **Seed data**:
    ```bash
    cd ra_dbt && uv run dbt seed
    
    # To refresh a specific seed with new schema:
    cd ra_dbt && uv run dbt seed --select raw_members --full-refresh
    ```

2.  **Run models**:
    ```bash
    cd ra_dbt && uv run dbt run
    ```

3.  **Test models**:
    ```bash
    cd ra_dbt && uv run dbt test
    ```
    
4.  **Or run everything (seed + run + test)**:
    ```bash
    cd ra_dbt && uv run dbt build
    ```

```text
                 (you)
                  |
                  v
         ┌───────────────────┐
         │  dbt project code  │
         │  - models/ (*.sql) │
         │  - seeds/ (*.csv)  │
         │  - tests/          │
         │  - snapshots/      │
         └─────────┬─────────┘
                   |
                   v
┌─────────────────────────────────────────────────────────┐
│                         DATABASE                        │
│                                                         │
│   dbt seed  ───────►  raw-ish tables from /seeds CSVs   │
│                     (loads CSVs into tables)            │
│                                                         │
│   dbt run   ───────►  models as views/tables            │
│                     (creates/updates m tell models only)|
│                                                         │
│   dbt build ───────►  seed + run + tests (+ snapshots*) │
│                     (end-to-end “make it right”)        │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## Warehouse Schemas

This project uses PostgreSQL schemas following the **Medallion Architecture** pattern to organize data transformations:

### dbt Layers (Build Order)

**1. `dbt_raw` - Bronze Layer (Seeds & Source Views)**
- **Purpose**: Raw, untransformed data loaded directly from CSV files (seeds) or source systems
- **Materialization**: Tables (seeds), Views (raw_views)
- **Contains**: 
  - Seeds: `raw_claims`, `raw_members`, `raw_enrollments`, `raw_plans`, `raw_providers`
  - Views: Year/scenario-filtered views like `raw_claims_2024`, `claims_2025_9months`
- **Example**: `raw_claims` contains raw claim records exactly as provided in the CSV

**2. `dbt_staging` - Silver Layer (Cleaned & Standardized)**
- **Purpose**: Type-safe, cleaned, standardized versions of raw data with basic transformations
- **Materialization**: Views
- **Contains**: `stg_claims_dx`, `stg_claims_rx`, `stg_enrollment`
- **Transformations**: 
  - Data type casting (e.g., strings to dates)
  - Column renaming for consistency
  - Basic data quality (lowercasing, trimming)
- **Example**: `stg_claims_dx` converts raw diagnosis strings to arrays and standardizes dates

**3. `dbt_intermediate` - Silver Layer (Business Logic)**
- **Purpose**: Business logic and aggregations that combine staging models
- **Materialization**: Views
- **Contains**: `member_diagnoses`, `member_rx`, `int_aca_risk_input`
- **Transformations**:
  - Join multiple staging models
  - Aggregate data per member (e.g., all diagnoses, all prescriptions)
  - Apply business rules
- **Example**: `int_aca_risk_input` joins member demographics with their aggregated diagnoses and prescriptions

**4. `dbt_marts` - Gold Layer (Analytics-Ready Outputs)**
- **Purpose**: Final, consumption-ready tables/views for analytics and reporting
- **Materialization**: Views
- **Contains**: `score_summary`, `hcc_summary`, `score_by_dim`, etc.
- **Note**: Replaces `public` schema for clearer ownership indication
- **Example**: `score_summary` provides aggregated risk score statistics per run

**5. `dbt_quality` - Data Quality Tests**
- **Purpose**: Views that validate data quality rules
- **Materialization**: Views
- **Contains**: `diagnosis_validity`, `member_completeness`, `enrollment_gaps`
- **Example**: `diagnosis_validity` checks if diagnosis codes are valid ICD-10 codes

### Dagster-Managed Schemas

**6. `dag_runs` - Atomic Execution Outputs**
- **Created by**: Dagster bootstrap (`uv run python -m ra_dagster`)
- **Written by**: Dagster scoring jobs
- **Contains**: 
  - `run_registry`: Audit log of every execution (run_id, config, timestamp, git info)
  - `risk_scores`: Member-level risk scores for each run
- **Example**: After scoring, `dag_runs.risk_scores` contains one row per member per run

**7. `dag_analytics` - Derived Analytics**
- **Created by**: Dagster bootstrap
- **Written by**: Dagster comparison and decomposition jobs
- **Contains**:
  - `run_comparison`: Delta analysis between two runs
  - `decomposition_definitions`: Decomposition scenario metadata
  - `decomposition_scenarios`: Decomposition driver breakdowns
- **Example**: `dag_analytics.run_comparison` shows member-level score differences between baseline and actual runs

### Flow Summary

```
dbt_raw (CSV seeds) 
  → dbt_staging (clean & type-safe)
    → dbt_intermediate (business logic)
      → dbt_marts (analytics-ready)
      
Dagster writes → dag_runs.risk_scores
Dagster reads dag_runs.risk_scores → dag_analytics.run_comparison
```

### Schema Ownership Summary

| Schema | Owner | Purpose |
|--------|-------|---------|
| `dbt_raw` | dbt | Seeds and source views |
| `dbt_staging` | dbt | Cleaned, standardized data |
| `dbt_intermediate` | dbt | Business logic aggregations |
| `dbt_marts` | dbt | Final analytics outputs |
| `dbt_quality` | dbt | Data quality validations |
| `dag_runs` | Dagster | Execution artifacts |
| `dag_analytics` | Dagster | Derived analytics |

## Data dictionary (Dagster tables)

Dagster-managed relations are documented in dbt via `sources` and `exposures`:

- `models/dagster_sources.yml`: `dag_runs.run_registry`, `dag_runs.risk_scores`, `dag_analytics.run_comparison`, `dag_analytics.decomposition_scenarios`, `dag_analytics.decomposition_definitions`
- `models/dagster_exposures.yml`: high-level Dagster pipeline dependencies

## Output

The `int_aca_risk_input` model will have the following schema:

| Column | Type | Description |
|--------|------|-------------|
| member_id | VARCHAR | Unique member ID |
| date_of_birth | DATE | DOB |
| gender | VARCHAR | M/F |
| metal_level | VARCHAR | Metal level |
| enrollment_months | INTEGER | Months enrolled |
| diagnoses | VARCHAR[] | List of ICD-10 codes |
| ndc_codes | VARCHAR[] | List of NDC codes |
