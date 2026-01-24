with members as (
    select 
        m.member_id, 
        r.run_ref as user_ref
    from {{ ref('int_run_member_dimensions') }} m
    join {{ source('dagster_runs', 'run_registry') }} r on m.run_id = r.run_id
),

diagnoses as (
    select 
        member_id, 
        array_length(diagnosis_list, 1) as diagnosis_count
    from {{ ref('int_member_diagnoses') }}
),

medications as (
    select 
        member_id, 
        array_length(ndc_list, 1) as medication_count
    from {{ ref('int_member_rx') }}
),

combined as (
    select
        m.member_id,
        m.user_ref,
        coalesce(d.diagnosis_count, 0) as diagnosis_count,
        coalesce(med.medication_count, 0) as medication_count,
        case
            when d.diagnosis_count = 0 then 'missing_diagnoses'
            when med.medication_count = 0 then 'missing_medications'
            else 'complete'
        end as quality_status,
        current_timestamp as checked_at
    from members m
    left join diagnoses d on m.member_id = d.member_id
    left join medications med on m.member_id = med.member_id
)

select * from combined
