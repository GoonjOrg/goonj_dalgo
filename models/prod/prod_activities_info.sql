{{ config(
    materialized='table',
    tags=['activities_info', 'prod', 'salesforce']
) }}

select 
    annual_year,
    month,
    monthnum,
    quarter,
    state,
    activity_start_date,
    activity_end_date,
    district,
    block,
    village,
    other_block,
    other_village,
    account_name,
    account_type,
    type_of_initiative,
    activity_type,
    activity_category,
    activity_sub_type,
    objective_of_cfw_work,
    number_of_activities,
    activity_id,
    num_cfw_female,
    num_cfw_male,
    num_cfw_others,
    num_working_days,
    num_njpc_female,
    num_njpc_male,
    num_njpc_others,
    num_njpc_days,
    num_s2s_participants,
    num_s2s_days,
    actvity_conducted_with_students,
    school_name,
    is_education_and_health
from 
{{ ref('int_activities') }}
