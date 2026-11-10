with raw_claims as (
    select *
    from {{ ref('raw_claims') }}
    where extract(year from cast(service_date as date)) = 2024
),

simulations as (
    -- Generate 50 simulation IDs
    select generate_series(1, 50) as sim_id
)

select 
    c.claim_id,
    c.provider_id,
    c.plan_id,
    c.service_date,
    c.claim_amount,
    c.allowed_amount,
    c.paid_amount,
    c.status,
    c.diagnosis_code,
    c.procedure_code,
    c.charges,
    c.allowed,
    c.clean_claim_status,
    c.claim_from,
    c.clean_claim_out,
    c.utilization,
    c.hcg_units_days,
    c.claim_type,
    c.major_service_category,
    c.provider_specialty,
    c.detailed_service_category,
    c.ms_drg,
    c.ms_drg_description,
    c.ms_drg_mdc,
    c.ms_drg_mdc_desc,
    c.cpt,
    c.cpt_consumer_description,
    c.procedure_level_1,
    c.procedure_level_2,
    c.procedure_level_3,
    c.procedure_level_4,
    c.procedure_level_5,
    c.channel,
    c.drug_name,
    c.drug_class,
    c.drug_subclass,
    c.drug,
    c.is_oon,
    c.best_contracting_entity_name,
    c.provider_group_name,
    c.ccsr_system_description,
    c.ccsr_description,
    -- Create unique member IDs for each simulation so the scorer treats them as distinct
    c.member_id || '_sim_' || s.sim_id as member_id,
    s.sim_id
from raw_claims c
cross join simulations s
-- The Monte Carlo Step: Keep claim if random roll is less than confidence
where random() < (
    case 
        -- Inpatient claims are usually well-documented
        when lower(claim_type) = 'inpatient' then 0.98
        -- Professional claims have moderate audit risk
        when lower(claim_type) = 'professional' then 0.85
        -- Pharmacy claims are very high confidence (electronic transaction)
        when lower(claim_type) = 'rx' then 0.99
        -- Everything else (e.g. DME, etc) gets lower confidence
        else 0.70 
    end
)
