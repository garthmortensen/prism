with base as (
    select *
    from {{ ref('raw_claims_2024') }}
),

-- Simulate "coding intensity": add an additional diagnosis code
-- to a subset of 2024 non-RX claims.
extra_dx as (
    select
        * exclude (diagnosis_code),
        'E11.9' as diagnosis_code
    from base
    where claim_type != 'RX'
      and claim_id like '%7'
)

select * from base
union all by name
select * from extra_dx
