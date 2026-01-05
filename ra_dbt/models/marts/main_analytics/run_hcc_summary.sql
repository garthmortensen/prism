with source as (
    select * from {{ source('dagster_runs_outputs', 'risk_scores') }}
),

unnested as (
    select
        run_id,
        unnest(from_json(components, '[{"component_type": "VARCHAR", "component_code": "VARCHAR", "coefficient": "DOUBLE"}]')) as component
    from source
),

hccs as (
    select
        run_id,
        component.component_code as hcc_code,
        component.coefficient as score_contribution
    from unnested
    where component.component_type = 'hcc'
),

run_totals as (
    select run_id, count(*) as total_members
    from source
    group by 1
)

select
    h.run_id,
    h.hcc_code,
    count(*) as member_count,
    count(*)::double / max(rt.total_members) as prevalence,
    avg(h.score_contribution) as avg_score_contribution,
    sum(h.score_contribution) as total_score_contribution
from hccs h
join run_totals rt on h.run_id = rt.run_id
group by 1, 2
