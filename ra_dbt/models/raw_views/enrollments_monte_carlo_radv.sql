with raw_enrollments as (
    select * 
    from {{ ref('raw_enrollments') }}
    where cast(start_date as date) <= date '2024-12-31'
      and cast(end_date as date) >= date '2024-01-01'
),

simulations as (
    select generate_series(1, 50) as sim_id
)

select 
    e.enrollment_id,
    e.plan_id,
    e.start_date,
    e.end_date,
    e.premium_paid,
    e.csr_variant,
    e.member_id || '_sim_' || s.sim_id as member_id,
    s.sim_id
from raw_enrollments e
cross join simulations s
