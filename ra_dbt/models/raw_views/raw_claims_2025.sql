select *
from {{ ref('raw_claims') }}
where extract(year from cast(service_date as date)) = 2025
