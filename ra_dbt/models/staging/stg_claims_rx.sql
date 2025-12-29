with source as (
    select * from {{ ref('raw_claims') }}
)

select
    claim_id,
    member_id,
    cast(service_date as date) as fill_date,
    -- Use the 'drug' column which contains NDC codes for RX claims
    drug as ndc_code
from source
where claim_type = 'RX'
