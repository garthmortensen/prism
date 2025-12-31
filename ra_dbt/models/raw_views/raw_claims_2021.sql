select *
from {{ ref('raw_claims') }}
where year(cast(service_date as date)) = 2021
