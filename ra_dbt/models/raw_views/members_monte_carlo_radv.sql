with raw_members as (
    select * 
    from {{ ref('raw_members') }}
    where cast(year as integer) = 2024
),

simulations as (
    select unnest(generate_series(1, 50)) as sim_id
)

select 
    m.* exclude (member_id),
    m.member_id || '_sim_' || s.sim_id as member_id,
    s.sim_id
from raw_members m
cross join simulations s
