with source as (
    select 
        *,
        -- Extract age and hcc_cnt from details JSON.
        try_cast(json_extract_string(details, '$.age') as int) as age,
        try_cast(json_extract_string(details, '$.hcc_cnt') as int) as hcc_cnt
    from {{ source('dagster_runs_outputs', 'risk_scores') }}
),

with_derived_dims as (
    select
        *,
        case
            when age < 18 then '0-17'
            when age < 30 then '18-29'
            when age < 40 then '30-39'
            when age < 50 then '40-49'
            when age < 60 then '50-59'
            when age < 65 then '60-64'
            else '65+'
        end as age_band,
        case
            when hcc_cnt = 0 then '0'
            when hcc_cnt = 1 then '1'
            when hcc_cnt = 2 then '2'
            when hcc_cnt >= 3 then '3+'
            else '0' -- Default to 0 if null/missing
        end as hcc_count_band,
        case
            when rxc_score > 0 then 'Yes'
            else 'No'
        end as has_rxc
    from source
),

unpivoted as (
    select run_id, 'model' as dimension_name, cast(model as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_id, 'gender' as dimension_name, cast(gender as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_id, 'metal_level' as dimension_name, cast(metal_level as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_id, 'enrollment_months' as dimension_name, cast(enrollment_months as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_id, 'age_band' as dimension_name, cast(age_band as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_id, 'hcc_count_band' as dimension_name, cast(hcc_count_band as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_id, 'has_rxc' as dimension_name, cast(has_rxc as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
)

select
    run_id,
    dimension_name,
    dimension_value,
    count(*) as member_count,
    avg(risk_score) as avg_score,
    min(risk_score) as min_score,
    max(risk_score) as max_score,
    quantile_cont(risk_score, 0.5) as p50,
    quantile_cont(risk_score, 0.9) as p90,
    quantile_cont(risk_score, 0.99) as p99,
    avg(hcc_score) as avg_hcc_score,
    avg(rxc_score) as avg_rxc_score,
    avg(demographic_score) as avg_demographic_score
from unpivoted
group by 1, 2, 3
