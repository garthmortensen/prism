select *
from {{ ref('raw_members') }}
where cast(year as integer) = 2025
