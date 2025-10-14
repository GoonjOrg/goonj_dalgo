{{ config(
    materialized='table',
    tags=['activities_summary_year', 'prod', 'salesforce']
) }}

select 
    annual_year,
    state,
    district,
    type_of_initiative,
    sum(distinct case account_type when 'Self' then number_of_activities else null end) as number_self_activites,
    sum(distinct case account_type when 'Partner' then number_of_activities else null end) as number_partner_activities,

    sum(number_of_activities) as num_activities,
    count(distinct activity_id) as activity_count
from 
{{ ref('int_activities') }}
Group by annual_year, state, district, type_of_initiative