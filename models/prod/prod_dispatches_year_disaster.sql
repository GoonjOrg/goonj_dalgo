{{ config(
    materialized='table',
    tags=['dispatches_by_year_disaster', 'prod', 'salesforce']
) }}



select
annual_year,
quarter,
month,
state,
district,
disaster_type,
kit_type,
count(distinct case receiver_account_type when 'Self' then dispatch_id else null end) as num_self_dispatches,
count(distinct case receiver_account_type when 'Partner' then dispatch_id else null end) as num_external_dispatches,
sum(quantity) as kit_count,
count(distinct dispatch_id) as total_dispatches
from 
{{ ref('int_dispatches') }}as dispatches
where disaster_type is not null and kit_type is not null
and disaster_type != 'Not Applicable'
group by annual_year, quarter, month, state, district, disaster_type, kit_type


