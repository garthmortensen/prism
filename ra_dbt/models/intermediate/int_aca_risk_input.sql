with members as (
    select * from {{ ref('int_member_months') }}
),

diagnoses as (
    select * from {{ ref('int_member_diagnoses') }}
),

rx as (
    select * from {{ ref('int_member_rx') }}
)

select
    m.member_id,
    m.enrollment_months,
    m.gender,
    m.metal_level,
    m.date_of_birth,
    coalesce(d.diagnosis_list, []) as diagnoses,
    coalesce(r.ndc_list, []) as ndc_codes
from members m
left join diagnoses d on m.member_id = d.member_id
left join rx r on m.member_id = r.member_id
