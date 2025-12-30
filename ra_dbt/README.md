# ra_dbt

This dbt project transforms raw enrollment and claims data into the input format required for the ACA Risk Calculator.

## Models

- **Staging**: Cleans raw data from seeds/sources.
- **Intermediate**: Aggregates diagnoses and NDCs per member.
- **Intermediate (final)**: `int_aca_risk_input` joins everything into a single relation ready for the Python calculator (Dagster reads from here).

## Running

1.  **Seed data**:
    ```bash
    uv run dbt seed
    ```

2.  **Run models**:
    ```bash
    uv run dbt run
    ```

3.  **Test models**:
    ```bash
    uv run dbt test
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
│                         DATABASE                         │
│                                                         │
│   dbt seed  ───────►  raw-ish tables from /seeds CSVs    │
│                     (loads CSVs into tables)             │
│                                                         │
│   dbt run   ───────►  models as views/tables             │
│                     (creates/updates models only)        │
│                                                         │
│   dbt build ───────►  seed + run + tests (+ snapshots*)  │
│                     (end-to-end “make it right”)         │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## Warehouse Schemas

This project uses DuckDB schemas to keep layers clear:

Note: DuckDB tooling may show `main_` prefixes because `main` is the default database.

- `raw`: dbt seeds (e.g., `raw_claims`)
- `staging`: dbt staging views (e.g., `stg_claims_dx`)
- `intermediate`: dbt intermediate views (including `int_aca_risk_input`)
- `main_runs` / `main_analytics`: created and written by Dagster (run artifacts + downstream analytics)

## Data dictionary (Dagster tables)

Dagster-managed relations are documented in dbt via `sources` and `exposures`:

- `models/dagster_sources.yml`: `main_runs.run_registry`, `main_runs.risk_scores`, `main_analytics.run_comparison`, `main_analytics.decomposition`
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
