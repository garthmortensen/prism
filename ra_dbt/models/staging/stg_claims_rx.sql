with source as (
    select * from {{ ref('raw_claims') }}
)

select
    claim_id,
    member_id,
    cast(service_date as date) as fill_date,
    -- Placeholder: raw_claims.csv does not currently have an NDC column
    cast(null as varchar) as ndc_code
from source
where claim_type = 'RX'
