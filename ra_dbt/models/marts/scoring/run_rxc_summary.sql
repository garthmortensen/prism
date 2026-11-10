with source as (
    select * from {{ source('dagster_runs_outputs', 'risk_scores') }}
),

runs as (
    select run_id, run_ref
    from {{ source('dagster_runs', 'run_registry') }}
),

unnested as (
    select
        run_id,
        jsonb_array_elements(components::jsonb) as component
    from source
),

rxcs as (
    select
        run_id,
        component->>'component_code' as rxc_code,
        (component->>'coefficient')::double precision as score_contribution
    from unnested
    where component->>'component_type' = 'rxc'
),

run_totals as (
    select run_id, count(*) as total_members
    from source
    group by 1
)

select
    ru.run_ref as user_ref,
    r.rxc_code,
    count(*) as member_count,
    count(*)::double precision / max(rt.total_members) as prevalence,
    avg(r.score_contribution) as avg_score_contribution,
    sum(r.score_contribution) as total_score_contribution
from rxcs r
join run_totals rt on r.run_id = rt.run_id
left join runs ru on r.run_id = ru.run_id
group by 1, 2
