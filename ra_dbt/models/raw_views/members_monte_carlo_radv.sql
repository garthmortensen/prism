with raw_members as (
    select * 
    from {{ ref('raw_members') }}
    where cast(year as integer) = 2024
),

simulations as (
    select generate_series(1, 50) as sim_id
)

select 
    m.first_name,
    m.last_name,
    m.dob,
    m.gender,
    m.email,
    m.phone,
    m.street,
    m.city,
    m.state,
    m.zip,
    m.fpl_ratio,
    m.hios_id,
    m.plan_network_access_type,
    m.plan_metal,
    m.age_group,
    m.region,
    m.enrollment_length_continuous,
    m.clinical_segment,
    m.general_agency_name,
    m.broker_name,
    m.sa_contracting_entity_name,
    m.call_count,
    m.app_login_count,
    m.web_login_count,
    m.new_member_in_period,
    m.member_used_app,
    m.member_had_web_login,
    m.member_visited_new_provider_ind,
    m.high_cost_member,
    m.mutually_exclusive_hcc_condition,
    m.geographic_reporting,
    m.wisconsin_area_deprivation_index,
    m.ra_mm,
    m.year,
    m.member_id || '_sim_' || s.sim_id as member_id,
    s.sim_id
from raw_members m
cross join simulations s
