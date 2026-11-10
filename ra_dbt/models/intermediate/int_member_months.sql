with enrollment as (
    select * from {{ ref('stg_enrollment') }}
)

select
    member_id,
    -- Simple approximation for months enrolled
    -- PostgreSQL: Calculate months between dates using EXTRACT and AGE
    least(12, greatest(1, 
        extract(year from age(end_date, start_date)) * 12 + 
        extract(month from age(end_date, start_date)) + 1
    )) as enrollment_months,
    gender,
    metal_level,
    date_of_birth
from enrollment
