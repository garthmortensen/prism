select *
from {{ ref('raw_enrollments') }}
where cast(start_date as date) <= date '2025-12-31'
  and cast(end_date as date) >= date '2025-01-01'
