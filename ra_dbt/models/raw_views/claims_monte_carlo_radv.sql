with raw_claims as (
    select *
    from {{ ref('raw_claims') }}
    where year(cast(service_date as date)) = 2024
),

simulations as (
    -- Generate 50 simulation IDs
    select unnest(generate_series(1, 50)) as sim_id
)

select 
    c.* exclude (member_id),
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
