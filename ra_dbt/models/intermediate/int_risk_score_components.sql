{{ config(
    materialized='view',
    schema='intermediate'
) }}

/*
    Normalized table that stores each risk score component individually.
    Each row represents a single score component (demographic, HCC, RXC, or HCC group)
    with full audit trail including source data and table references.
*/

WITH score_data AS (
    SELECT
        rs.member_id,
        rs.run_timestamp,
        rs.calculator,
        rs.model_year,
        rs.risk_score,
        rs.components
    FROM {{ source('marts', 'risk_scores') }} rs
    WHERE rs.components IS NOT NULL
),

exploded_components AS (
    SELECT
        member_id,
        run_timestamp,
        calculator,
        model_year,
        risk_score,
        unnest(components) AS component
    FROM score_data
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

SELECT
    member_id,
    run_timestamp,
    calculator,
    model_year,
    risk_score,
    
    -- Component details
    component_type,
    component_code,
    coefficient_value,
    
    -- Source lineage
    source_diagnoses_or_ndcs,
    
    -- Hierarchy tracking
    was_superseded_by,
    superseded_components,
    is_part_of_group,
    
    -- Metadata
    model_type,
    metal_level,
    table_references,
    created_at
    
FROM parsed_components
ORDER BY
    member_id,
    run_timestamp,
    component_type,
    component_code
