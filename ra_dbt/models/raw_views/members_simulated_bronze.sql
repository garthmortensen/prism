select * exclude (plan_metal),
       'Bronze' as plan_metal
from {{ ref('raw_members') }}
where cast(year as integer) = 2024
