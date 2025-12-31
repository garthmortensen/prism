with enrollments as (
    select * from {{ ref('raw_enrollments') }}
),

members as (
    select * from {{ ref('raw_members') }}
)

select
    e.member_id,
    cast(e.start_date as date) as start_date,
    cast(e.end_date as date) as end_date,
    m.gender,
    lower(m.plan_metal) as metal_level,
    cast(m.dob as date) as date_of_birth
from enrollments e
left join members m on e.member_id = m.member_id
