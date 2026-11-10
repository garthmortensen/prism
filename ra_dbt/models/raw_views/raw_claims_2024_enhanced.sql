with base as (
    select *
    from {{ ref('raw_claims_2024') }}
),

-- Simulate "coding intensity": add an additional diagnosis code
-- to a subset of 2024 non-RX claims.
extra_dx as (
    select
        claim_id,
        member_id,
        provider_id,
        plan_id,
        service_date,
        claim_amount,
        allowed_amount,
        paid_amount,
        status,
        'E11.9' as diagnosis_code,
        procedure_code,
        charges,
        allowed,
        clean_claim_status,
        claim_from,
        clean_claim_out,
        utilization,
        hcg_units_days,
        claim_type,
        major_service_category,
        provider_specialty,
        detailed_service_category,
        ms_drg,
        ms_drg_description,
        ms_drg_mdc,
        ms_drg_mdc_desc,
        cpt,
        cpt_consumer_description,
        procedure_level_1,
        procedure_level_2,
        procedure_level_3,
        procedure_level_4,
        procedure_level_5,
        channel,
        drug_name,
        drug_class,
        drug_subclass,
        drug,
        is_oon,
        best_contracting_entity_name,
        provider_group_name,
        ccsr_system_description,
        ccsr_description
    from base
    where claim_type != 'RX'
      and claim_id like '%7'
)

select * from base
union all
select * from extra_dx
