{{ config(
    materialized='table',
    tags=['distribution_year_disaster', 'prod', 'salesforce']
) }}


SELECT
    distributions.annual_year,
    distributions.disaster_type,
    sum(case when kit_type in ('CFW','DFW') then  quantity else 0 end) as cfw_kit_count,
    sum(case kit_type when 'My Pad Woman' then  quantity else 0 end) as njpc_kit_count,
    sum(case when kit_type in ('S2S','S2S-AW') then  quantity else 0 end) as s2s_kit_count,
    sum(case when kit_type not in ('CFW', 'My Pad Woman', 'S2S', 'S2S-AW') then quantity else 0 end) as other_kit_count
    
FROM 
{{ ref('int_distributions') }} as distributions 
where distributions.is_deleted=False 
Group by distributions.annual_year, distributions.disaster_type