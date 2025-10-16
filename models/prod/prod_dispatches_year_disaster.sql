{{ config(
    materialized='table',
    tags=['dispatches_by_year_disaster', 'prod', 'salesforce']
) }}



select
annual_year,
state,
district,
disaster_type,
kit_type,
--count(distinct case dispatched_account_type when 'Self' then dispatch_id else null end) as num_self_dispatches,
--count(distinct case dispatched_account_type when 'Partner' then dispatch_id else null end) as num_external_dispatches,
--sum(case when kit_type in ('CFW','DFW') then  quantity else 0 end) as cfw_kit_count,
--sum(case kit_type when 'My Pad Woman' then  quantity else 0 end) as njpc_kit_count,
--sum(case when kit_type in ('S2S','S2S-AW') then  quantity else 0 end) as s2s_kit_count,
--sum(case when kit_type not in ('CFW','DFW', 'My Pad Woman', 'S2S', 'S2S-AW') then quantity else 0 end) as other_kit_count,
sum(quantity) as kit_count
--count(distinct dispatch_id) as total_dispatches
from 
{{ ref('int_dispatches') }}as dispatches
where disaster_type is not null and kit_type is not null
and disaster_type != 'Not Applicable'

group by annual_year, state,district, disaster_type,kit_type

