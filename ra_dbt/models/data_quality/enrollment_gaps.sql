with enrollments as (
    select * from {{ ref('stg_enrollment') }}
),

gaps as (
    select
        member_id,
        lag(end_date) over (partition by member_id order by start_date) as prev_end,
        start_date,
        (start_date - lag(end_date) over (partition by member_id order by start_date)) as gap_days
    from enrollments
),

gap_analysis as (
    select
        member_id,
        prev_end,
        start_date,
        gap_days,
        case
            when gap_days > 0 then 'gap_detected'
            when gap_days = 0 then 'consecutive'
            when gap_days < 0 then 'overlapping_error'
            else 'first_period'
        end as gap_status
    from gaps
    where gap_days is not null and gap_days > 0
)

select * from gap_analysis
