with source as (
    select * from {{ source('dagster_runs_outputs', 'risk_scores') }}
),

runs as (
    select run_id, run_ref
    from {{ source('dagster_runs', 'run_registry') }}
),

binned as (
    select
        r.run_ref as user_ref,
        floor(risk_score * 10) / 10 as score_bucket,
        count(*) as member_count,
        avg(risk_score) as avg_score,
        min(risk_score) as min_score,
        max(risk_score) as max_score,
        avg(hcc_score) as avg_hcc_score,
        avg(rxc_score) as avg_rxc_score,
        avg(demographic_score) as avg_demographic_score
    from source s
    left join runs r on s.run_id = r.run_id
    group by 1, 2
),

with_totals as (
    select
        *,
        sum(member_count) over (partition by user_ref) as total_run_members
    from binned
)

select
    user_ref,
    score_bucket,
    member_count,
    member_count::double precision / total_run_members as pct_members,
    avg_score,
    min_score,
    max_score,
    avg_hcc_score,
    avg_rxc_score,
    avg_demographic_score
from with_totals
