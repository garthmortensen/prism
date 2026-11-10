with diagnoses as (
    select 
        member_id,
        unnest(diagnosis_list) as diagnosis_code
    from {{ ref('int_member_diagnoses') }}
),

invalid_diagnoses as (
    select
        member_id,
        diagnosis_code,
        case
            when diagnosis_code is null then 'null_code'
            when length(diagnosis_code) < 3 then 'too_short'
            when length(diagnosis_code) > 7 then 'too_long'
            when diagnosis_code ~ '^[A-Z][0-9][A-Z0-9\.]*$' then 'valid_icd10'
            else 'invalid_format'
        end as validity_issue
    from diagnoses
    where diagnosis_code is null
        or length(diagnosis_code) < 3
        or length(diagnosis_code) > 7
        or not (diagnosis_code ~ '^[A-Z][0-9][A-Z0-9\.]*$')
)

select * from invalid_diagnoses
