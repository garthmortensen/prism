select *
from {{ ref('raw_claims') }}
where extract(year from cast(service_date as date)) = 2025
  and cast(clean_claim_out as date) <= '2025-09-30'
