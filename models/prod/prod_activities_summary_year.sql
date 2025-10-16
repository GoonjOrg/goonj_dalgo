{{ config(
    materialized='table',
    tags=['activities_summary_year', 'prod', 'salesforce']
) }}

select 
    annual_year,
    state,
    district,
    type_of_initiative,
    sum(case account_type when 'Self' then number_of_activities else 0 end) as num_self_activities,
    sum(case account_type when 'Partner' then number_of_activities else 0 end) as num_partner_activities,

    sum(number_of_activities) as num_activities,
    count(distinct activity_id) as activity_count
from 
{{ ref('int_activities') }}
Group by annual_year, state, district, type_of_initiative