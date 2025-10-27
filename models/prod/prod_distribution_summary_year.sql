{{ config(
    materialized='table',
    tags=['distribution_summary_year', 'prod', 'salesforce']
) }}

with annual_new_geo as (
    select 
        annual_year,
        count(distinct case when new_state = true then state end) as new_state_count,
        count(distinct case when new_district = true then concat(state, '|', district) end) as new_district_count
    from {{ ref('int_distribution_geography') }}
    group by annual_year
)


SELECT
    distributions.annual_year,
    --distributions.disaster_type,
    count(distinct state) as state_count,
    count(distinct (state,district)) as district_count,
    count(distinct (state,district,block)) as block_count,
    count(distinct (state,district,block,village)) as village_count,
    count(distinct (state,district, other_block)) as other_block_count,
    count(distinct (state,district,block,other_block,other_village))as other_village_count,
    count(case type_of_initiative when 'Only CFW' then 1 else null end) as cfwcount,
    count(case type_of_initiative when 'Only Rahat' then 1 else null end) as rahatcount,
    count(case type_of_initiative when 'Only S2S' then 1 else null end) as s2scount,
    count(case type_of_initiative when 'Only NJPC' then 1 else null end) as njpccount,
    count(case type_of_initiative when 'CFW-Rahat' then 1 else null end) as cfwrahatcount,
    count(case type_of_initiative when 'CFW-S2S' then 1 else null end) as cfws2scount,
    count(case type_of_initiative when 'CFW-NJPC' then 1 else null end) as cfwnjpccount,
    count(case type_of_initiative when 'Education and Health' then 1 else null end) as education_and_healthcount,
    count(case type_of_initiative when 'Vapsi' then 1 else null end) as vapsicount,
    count(case type_of_initiative when 'Specific Initiative' then 1 else null end) as specific_initiativecount, 
    sum(case kit_type when in ('CFW','DFW') then  quantity else 0 end) as cfw_kit_count,
    sum(case kit_type when 'My Pad Woman' then  quantity else 0 end) as njpc_kit_count,
    sum(case kit_type when ('S2S','S2S-AW') then  quantity else 0 end) as s2s_kit_count,
    sum(case when kit_type not in ('CFW', 'My Pad Woman', 'S2S', 'S2S-AW') then quantity else 0 end) as other_kit_count,
    count(distinct case distributor_account_type when 'Self' then distribution_id else null end) as num_self_distributions,
    count(distinct case distributor_account_type when 'Partner' then distribution_id else null end) as num_partner_distributions,
    annual_new_geo.new_state_count,
    annual_new_geo.new_district_count
    
FROM 
{{ ref('int_distributions') }} as distributions join annual_new_geo on distributions.annual_year=annual_new_geo.annual_year
where distributions.is_deleted=False 
Group by distributions.annual_year, annual_new_geo.new_state_count, annual_new_geo.new_district_count