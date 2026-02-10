{{ config(
    materialized='table',
    tags=['njpc_activities', 'prod', 'salesforce']
) }}

select distinct
    annual_year,
    month,
    monthnum,
    quarter,
    state,
    district,
    account_name,
    account_type,
    type_of_initiative,
    is_education_and_health,
    sum(number_of_activities) as num_activities,
    count(distinct activity_id) as activity_count,
    sum(num_njpc_female) as num_njpc_female,
    sum(num_njpc_male) as num_njpc_male,
    sum(num_njpc_others) as num_njpc_others,
    sum(num_njpc_days) as num_njpc_days,
    sum(num_s2s_participants) as num_s2s_participants
    
from 
{{ ref('int_activities') }}
where type_of_initiative='NJPC'
Group by annual_year, month, monthnum,quarter, state, district, account_name, account_type, type_of_initiative,is_education_and_health