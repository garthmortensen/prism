with source as (
    select * from {{ ref('raw_claims') }}
)

select
    claim_id,
    member_id,
    cast(service_date as date) as service_date,
    replace(diagnosis_code, '.', '') as diagnosis_code
from source
where diagnosis_code is not null
  and claim_type != 'RX'
