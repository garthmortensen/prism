select *
from {{ ref('raw_claims') }}
where extract(year from cast(service_date as date)) = 2024
  and cast(clean_claim_out as date) <= '2024-06-30'
