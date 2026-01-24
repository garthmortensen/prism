with source as (
    select 
        *,
        (details->>'age')::int as age
    from {{ source('dagster_runs_outputs', 'risk_scores') }}
),

runs as (
    select run_id, run_ref
    from {{ source('dagster_runs', 'run_registry') }}
),

aggregated as (
    select
        r.run_ref as user_ref,
        count(*) as member_count,
        avg(risk_score) as avg_score,
        min(risk_score) as min_score,
        max(risk_score) as max_score,
        percentile_cont(0.5) within group (order by risk_score) as p50,
        percentile_cont(0.9) within group (order by risk_score) as p90,
        percentile_cont(0.99) within group (order by risk_score) as p99,
        avg(age) as avg_age,
                 -- HCC metrics
        avg(hcc_score) as avg_hcc_score,
        min(hcc_score) as min_hcc_score,
        max(hcc_score) as max_hcc_score,
        stddev(hcc_score) as std_dev_hcc_score,
        
        -- RXC metrics
        avg(rxc_score) as avg_rxc_score,
        min(rxc_score) as min_rxc_score,
        max(rxc_score) as max_rxc_score,
        stddev(rxc_score) as std_dev_rxc_score,
        
        -- Demographic metrics
        avg(demographic_score) as avg_demographic_score,
        min(demographic_score) as min_demographic_score,
        max(demographic_score) as max_demographic_score,
        stddev(demographic_score) as std_dev_demographic_score

    from source s
    left join runs r on s.run_id = r.run_id
    group by 1
)

select * from aggregated
