with rx as (
    select * from {{ ref('stg_claims_rx') }}
)

select
    member_id,
    array_agg(distinct ndc_code) as ndc_list
from rx
group by member_id
