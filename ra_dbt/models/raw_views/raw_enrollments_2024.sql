select *
from {{ ref('raw_enrollments') }}
where cast(start_date as date) <= date '2024-12-31'
  and cast(end_date as date) >= date '2024-01-01'
