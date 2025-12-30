{{ config(
    materialized='view',
    schema='intermediate'
) }}

/*
    WHAT THIS MODEL DOES:
    ---------------------
    Explodes the JSON `components` array from `main_runs.risk_scores` into individual rows,
    one per score component (demographic, HCC, RXC, HCC group, EDF).
    
    This gives you a granular audit trail showing exactly which diagnosis codes, NDC codes,
    or demographic factors contributed to each member's final risk score.
    
    WHY THE CONDITIONAL LOGIC:
    --------------------------
    This model depends on Dagster writing `main_runs.risk_scores` with a `components` column.
    
    - If you run `dbt build` before running the Dagster scoring pipeline, that table won't exist yet.
    - Without defensive checks, dbt would error: "Table does not exist!"
    
    SOLUTION (Input Stubbing):
    --------------------------
    1. At compile time, we check if the source table exists and has the `components` column.
    2. If YES: Query the real data.
    3. If NO: Return an empty result set with the correct column types (WHERE 1=0).
    
    This allows dbt to compile and test successfully even in a fresh warehouse,
    while still producing real data once Dagster has run at least once.
    
    The rest of the transformation logic (unnest, json_extract) runs unconditionally,
    so you maintain a single source of truth for the output schema.
*/

{%- set source_ref = source('dagster_runs_outputs', 'risk_scores') -%}
{%- set relation = adapter.get_relation(
    database=source_ref.database,
    schema=source_ref.schema,
    identifier=source_ref.identifier
) -%}

{# Check if table exists AND has the required 'components' column #}
{%- set source_is_ready = False -%}
{%- if relation is not none -%}
    {%- set cols = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}
    {%- if 'components' in cols -%}
        {%- set source_is_ready = True -%}
    {%- endif -%}
{%- endif -%}

WITH source_data AS (
    {%- if source_is_ready %}
    SELECT
        member_id,
        run_timestamp,
        calculator,
        model_year,
        risk_score,
        components
    FROM {{ source_ref }}
    WHERE components IS NOT NULL
    {%- else %}
    -- Fallback: Generate an empty input set if the source table/column is missing.
    -- This allows the downstream logic to compile and run (producing 0 rows)
    -- without needing to hardcode the final output schema here.
    SELECT
        CAST(NULL AS VARCHAR) AS member_id,
        CAST(NULL AS VARCHAR) AS run_timestamp,
        CAST(NULL AS VARCHAR) AS calculator,
        CAST(NULL AS VARCHAR) AS model_year,
        CAST(NULL AS DOUBLE) AS risk_score,
        CAST(NULL AS JSON) AS components
    WHERE 1 = 0
    {%- endif %}
),

exploded_components AS (
    SELECT
        member_id,
        run_timestamp,
        calculator,
        model_year,
        risk_score,
        unnest(CAST(components AS JSON[])) AS component
    FROM source_data
),

parsed_components AS (
    SELECT
        member_id,
        run_timestamp,
        calculator,
        model_year,
        risk_score,
        
        -- Component identification
        json_extract_string(component, '$.component_type') AS component_type,
        json_extract_string(component, '$.component_code') AS component_code,
        CAST(json_extract_string(component, '$.coefficient') AS DECIMAL(10,4)) AS coefficient_value,
        
        -- Source data lineage
        json_extract(component, '$.source_data') AS source_diagnoses_or_ndcs,
        
        -- Hierarchy tracking
        json_extract_string(component, '$.superseded_by') AS was_superseded_by,
        json_extract(component, '$.supersedes') AS superseded_components,
        json_extract_string(component, '$.grouped_into') AS is_part_of_group,
        
        -- Calculation metadata
        json_extract(component, '$.table_references') AS table_references,
        json_extract_string(json_extract(component, '$.table_references'), '$.model') AS model_type,
        json_extract_string(json_extract(component, '$.table_references'), '$.metal_level') AS metal_level,
        
        -- Timestamp
        current_timestamp AS created_at
        
    FROM exploded_components
)

SELECT * FROM parsed_components
ORDER BY
    member_id,
    run_timestamp,
    component_type,
    component_code
