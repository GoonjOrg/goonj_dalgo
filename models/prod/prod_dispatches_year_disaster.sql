{{ config(
    materialized='table',
    tags=['dispatches_by_year_disaster', 'prod', 'salesforce']
) }}



select
annual_year,
state,
disaster_type,
count(distinct case dispatched_account_type when 'Self' then dispatch_id else null end) as num_self_dispatches,
count(distinct case dispatched_account_type when 'Partner' then dispatch_id else null end) as num_external_dispatches,
sum(case kit_type when 'CFW' then  quantity else 0 end) as cfw_kit_count,
sum(case kit_type when 'My Pad Woman' then  quantity else 0 end) as njpc_kit_count,
sum(case kit_type when 'S2S' then  quantity else 0 end) as s2s_kit_count,
sum(case kit_type when 'S2S-AW' then  quantity else 0 end) as s2s_aw_kit_count,
sum(case when kit_type not in ('CFW', 'My Pad Woman', 'S2S', 'S2S-AW') then quantity else 0 end) as other_kit_count,
sum(quantity) as total_kit_count,
count(distinct dispatch_id) as total_dispatches
from 
{{ ref('int_dispatches') }}as dispatches
group by annual_year, state, disaster_type

