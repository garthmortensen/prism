with comparison as (
    select * from {{ source('dagster_analytics', 'run_comparison') }}
),

runs as (
    select run_id, run_ref
    from {{ source('dagster_runs', 'run_registry') }}
),

dims as (
    select * from {{ ref('int_run_member_dimensions') }}
),

comparison_with_dims as (
    select 
        c.batch_id,
        ra.run_ref as user_ref_a,
        rb.run_ref as user_ref_b,
        c.member_id,
        c.match_status,
        c.score_diff,
        c.score_a,
        c.score_b,
        -- Coalesce dimensions from B, then A (to handle added/removed members)
        coalesce(dim_b.model, dim_a.model) as model,
        coalesce(dim_b.gender, dim_a.gender) as gender,
        coalesce(dim_b.metal_level, dim_a.metal_level) as metal_level,
        coalesce(dim_b.enrollment_months, dim_a.enrollment_months) as enrollment_months,
        coalesce(dim_b.age_band, dim_a.age_band) as age_band,
        coalesce(dim_b.hcc_count_band, dim_a.hcc_count_band) as hcc_count_band,
        coalesce(dim_b.has_rxc, dim_a.has_rxc) as has_rxc
    from comparison c
    left join dims dim_a on c.run_id_a = dim_a.run_id and c.member_id = dim_a.member_id
    left join dims dim_b on c.run_id_b = dim_b.run_id and c.member_id = dim_b.member_id
    left join runs ra on c.run_id_a = ra.run_id
    left join runs rb on c.run_id_b = rb.run_id
),

unpivoted as (
    select batch_id, user_ref_a, user_ref_b, 'model' as dimension_name, cast(model as varchar) as dimension_value, match_status, score_diff, score_a, score_b from comparison_with_dims
    union all
    select batch_id, user_ref_a, user_ref_b, 'gender' as dimension_name, cast(gender as varchar) as dimension_value, match_status, score_diff, score_a, score_b from comparison_with_dims
    union all
    select batch_id, user_ref_a, user_ref_b, 'metal_level' as dimension_name, cast(metal_level as varchar) as dimension_value, match_status, score_diff, score_a, score_b from comparison_with_dims
    union all
    select batch_id, user_ref_a, user_ref_b, 'enrollment_months' as dimension_name, cast(enrollment_months as varchar) as dimension_value, match_status, score_diff, score_a, score_b from comparison_with_dims
    union all
    select batch_id, user_ref_a, user_ref_b, 'age_band' as dimension_name, cast(age_band as varchar) as dimension_value, match_status, score_diff, score_a, score_b from comparison_with_dims
    union all
    select batch_id, user_ref_a, user_ref_b, 'hcc_count_band' as dimension_name, cast(hcc_count_band as varchar) as dimension_value, match_status, score_diff, score_a, score_b from comparison_with_dims
    union all
    select batch_id, user_ref_a, user_ref_b, 'has_rxc' as dimension_name, cast(has_rxc as varchar) as dimension_value, match_status, score_diff, score_a, score_b from comparison_with_dims
)

select
    batch_id,
    user_ref_a,
    user_ref_b,
    dimension_name,
    dimension_value,
    
    -- Counts
    count(*) as total_members,
    count(case when match_status = 'matched' then 1 end) as matched_count,
    count(case when match_status = 'b_only' then 1 end) as added_count,
    count(case when match_status = 'a_only' then 1 end) as removed_count,

    -- Score Metrics (Matched Only) - "Model Impact"
    avg(case when match_status = 'matched' then score_diff end) as avg_score_diff,
    min(case when match_status = 'matched' then score_diff end) as min_score_diff,
    max(case when match_status = 'matched' then score_diff end) as max_score_diff,
    
    -- Score Metrics (Portfolio Shift)
    avg(case when match_status = 'b_only' then score_b end) as avg_score_added,
    avg(case when match_status = 'a_only' then score_a end) as avg_score_removed

from unpivoted
group by 1, 2, 3, 4, 5
