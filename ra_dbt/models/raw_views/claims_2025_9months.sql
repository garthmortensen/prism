select *
from {{ ref('raw_claims') }}
where year(cast(service_date as date)) = 2025
  and cast(clean_claim_out as date) <= '2025-09-30'
