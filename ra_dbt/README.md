# Prism dbt Project

This dbt project transforms raw enrollment and claims data into the input format required for the ACA Risk Calculator.

## Models

- **Staging**: Cleans raw data from seeds/sources.
- **Intermediate**: Aggregates diagnoses and NDCs per member.
- **Marts**: `mart_aca_risk_input` joins everything into a single table ready for the Python calculator.

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

## Warehouse Schemas

This project uses DuckDB schemas to keep layers clear:

- `raw`: dbt seeds (e.g., `raw_claims`)
- `staging`: dbt staging views (e.g., `stg_claims_dx`)
- `mart`: dbt final input table for the Python calculator (`mart_aca_risk_input`)
- `meta` / `marts`: created and written by Dagster (run registry + scored outputs)

## Output

The final model `mart_aca_risk_input` will have the following schema:

| Column | Type | Description |
|--------|------|-------------|
| member_id | VARCHAR | Unique member ID |
| date_of_birth | DATE | DOB |
| gender | VARCHAR | M/F |
| metal_level | VARCHAR | Metal level |
| enrollment_months | INTEGER | Months enrolled |
| diagnoses | VARCHAR[] | List of ICD-10 codes |
| ndc_codes | VARCHAR[] | List of NDC codes |
