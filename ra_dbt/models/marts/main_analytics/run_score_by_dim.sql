with source as (
    select 
        *,
        -- Extract age and hcc_cnt from details JSON.
        (details->>'age')::int as age,
        (details->>'hcc_cnt')::int as hcc_cnt
    from {{ source('dagster_runs_outputs', 'risk_scores') }}
),

runs as (
    select run_id, run_ref
    from {{ source('dagster_runs', 'run_registry') }}
),

with_derived_dims as (
    select
        s.*,
        r.run_ref,
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
    from source s
    left join runs r on s.run_id = r.run_id
),

unpivoted as (
    select run_ref as user_ref, 'model' as dimension_name, cast(model as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_ref as user_ref, 'gender' as dimension_name, cast(gender as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_ref as user_ref, 'metal_level' as dimension_name, cast(metal_level as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_ref as user_ref, 'enrollment_months' as dimension_name, cast(enrollment_months as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_ref as user_ref, 'age_band' as dimension_name, cast(age_band as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_ref as user_ref, 'hcc_count_band' as dimension_name, cast(hcc_count_band as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
    union all
    select run_ref as user_ref, 'has_rxc' as dimension_name, cast(has_rxc as varchar) as dimension_value, risk_score, hcc_score, rxc_score, demographic_score from with_derived_dims
)

select
    user_ref,
    dimension_name,
    dimension_value,
    count(*) as member_count,
    avg(risk_score) as avg_score,
    min(risk_score) as min_score,
    max(risk_score) as max_score,
    percentile_cont(0.5) within group (order by risk_score) as p50,
    percentile_cont(0.9) within group (order by risk_score) as p90,
    percentile_cont(0.99) within group (order by risk_score) as p99,
    avg(hcc_score) as avg_hcc_score,
    avg(rxc_score) as avg_rxc_score,
    avg(demographic_score) as avg_demographic_score
from unpivoted
group by 1, 2, 3
