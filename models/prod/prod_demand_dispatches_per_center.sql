{{ config(
    materialized='table',
    tags=['demand_dispatches_per_center', 'prod', 'salesforce']
) }}



select
demands.annual_year,
dispatches.receiver_center_name,
demands.disaster_type,
demands.dispatch_stage,
demands.internal_demand,
count(distinct case receiver_account_type when 'Self' then dispatch_id else null end) as num_self_dispatches,
count(distinct case receiver_account_type when 'Partner' then dispatch_id else null end) as num_partner_dispatches,
sum(quantity) as total_kit_count,
count(distinct dispatch_id) as total_dispatches,
count(distinct demands.demand_id) as total_demands,
count(distinct case when demands.dispatch_stage = 'In Transit' then dispatch_id else null end) as num_intransit,
count(distinct case when demands.dispatch_stage = 'Reached' then dispatch_id else null end) as num_reached,
count(distinct case when demands.dispatch_stage  not in('In Transit','Reached') then dispatch_id else null end) as num_dispatchinprogress,
count(distinct case when dispatch_id is null then demands.demand_id else null end ) as num_demand_inprocess
from 
{{ ref('int_demands') }}as demands 
left JOIN {{ ref('int_dispatches') }} as dispatches on dispatches.demand_id = demands.demand_id
where demands.demand_status!='Closed' and demands.post_validation_status!='Closed'
group by demands.annual_year, dispatches.receiver_center_name, demands.disaster_type, demands.dispatch_stage,demands.internal_demand

