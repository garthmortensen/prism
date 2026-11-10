with diagnoses as (
    select * from {{ ref('stg_claims_dx') }}
)

select
    member_id,
    array_agg(distinct diagnosis_code) as diagnosis_list
from diagnoses
group by member_id
