with source as (
    select 
        *,
        -- Extract age and hcc_cnt from details JSON.
        (details->>'age')::int as age,
        (details->>'hcc_cnt')::int as hcc_cnt
    from {{ source('dagster_runs_outputs', 'risk_scores') }}
)

select
    run_id,
    member_id,
    -- Dimensions
    cast(model as varchar) as model,
    cast(gender as varchar) as gender,
    cast(metal_level as varchar) as metal_level,
    cast(enrollment_months as varchar) as enrollment_months,
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
