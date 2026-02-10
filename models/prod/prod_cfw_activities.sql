{{ config(
    materialized='table',
    tags=['cfw_activities', 'prod', 'salesforce']
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
    activity_type,
    activity_category,
    activity_sub_type,
    objective_of_cfw_work,
    sum(number_of_activities) as num_activities,
    count(distinct activity_id) as activity_count,
    sum(num_cfw_female) as num_cfw_female,
    sum(num_cfw_male) as num_cfw_male,
    sum(num_cfw_others) as num_cfw_others,
    sum(num_working_days) as num_working_days
from 
{{ ref('int_activities') }}
where type_of_initiative='CFW'
Group by annual_year, month, monthnum,quarter, state, district, account_name, account_type, type_of_initiative, activity_type, activity_category, activity_sub_type, objective_of_cfw_work