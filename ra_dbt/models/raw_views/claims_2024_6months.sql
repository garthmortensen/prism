select *
from {{ ref('raw_claims') }}
where year(cast(service_date as date)) = 2024
  and cast(clean_claim_out as date) <= '2024-06-30'
