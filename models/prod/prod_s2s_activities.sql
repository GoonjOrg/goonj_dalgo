{{ config(
    materialized='table',
    tags=['s2s_activities', 'prod', 'salesforce']
) }}

select 
    annual_year,
    month,
    quarter,
    state,
    district,
    account_name,
    account_type,
    type_of_initiative,
    sum(number_of_activities) as num_activities,
    count(distinct activity_id) as activity_count,
    sum(num_s2s_days) as num_s2s_days,
    sum(num_s2s_participants) as num_s2s_participants
    
from 
{{ ref('int_activities') }}
where type_of_initiative='S2S'
Group by annual_year, month, quarter, state, district, account_name, account_type, type_of_initiative