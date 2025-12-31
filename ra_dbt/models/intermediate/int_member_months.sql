with enrollment as (
    select * from {{ ref('stg_enrollment') }}
)

select
    member_id,
    -- Simple approximation for months enrolled
    -- DuckDB date_diff returns integer difference in months
    least(12, greatest(1, date_diff('month', start_date, end_date) + 1)) as enrollment_months,
    gender,
    metal_level,
    date_of_birth
from enrollment
