with diagnoses as (
    select * from {{ ref('stg_claims_dx') }}
)

select
    member_id,
    list(distinct diagnosis_code) as diagnosis_list
from diagnoses
group by member_id
