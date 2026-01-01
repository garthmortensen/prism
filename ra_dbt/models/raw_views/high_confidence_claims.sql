select *
from {{ ref('raw_claims') }}
where year(cast(service_date as date)) = 2024
  -- Simulate excluding "Low Confidence" claims (e.g. Telehealth, Urgent Care)
  -- In a real dataset, this would filter on place_of_service or specific procedure codes.
  -- Here we simulate it by excluding a subset of claims (approx 10%).
  and claim_id not like '%9'
