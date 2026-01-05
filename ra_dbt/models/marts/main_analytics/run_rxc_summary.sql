with source as (
    select * from {{ source('dagster_runs_outputs', 'risk_scores') }}
),

unnested as (
    select
        run_id,
        unnest(from_json(components, '[{"component_type": "VARCHAR", "component_code": "VARCHAR", "coefficient": "DOUBLE"}]')) as component
    from source
),

rxcs as (
    select
        run_id,
        component.component_code as rxc_code,
        component.coefficient as score_contribution
    from unnested
    where component.component_type = 'rxc'
),

run_totals as (
    select run_id, count(*) as total_members
    from source
    group by 1
)

select
    r.run_id,
    r.rxc_code,
    count(*) as member_count,
    count(*)::double / max(rt.total_members) as prevalence,
    avg(r.score_contribution) as avg_score_contribution,
    sum(r.score_contribution) as total_score_contribution
from rxcs r
join run_totals rt on r.run_id = rt.run_id
group by 1, 2
