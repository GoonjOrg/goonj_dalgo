{{ config(
    materialized='table',
    tags=['activities_distribution', 'prod', 'salesforce']
) }}

select 
    a.annual_year,
    a.month,
    a.monthnum,
    a.quarter,
    a.state,
    a.district,
    a.account_name,
    a.account_type,
    a.type_of_initiative,
    d.type_of_initiative as distribution_initiative,
    a.created_by,
    a.created_date,
    activity_type,
    activity_category,
    activity_sub_type,
    activity_start_date,
    activity_end_date,
    a.activity_id,
    is_education_and_health,
    number_of_activities
    num_working_days,
    num_cfw_female,
    num_cfw_male,
    num_cfw_others,
    actvity_conducted_with_students,
    num_s2s_participants,
    num_s2s_days,
    num_njpc_days,
    num_njpc_female,
    num_njpc_male,
    num_njpc_others,
    distribution_id,
    date_of_distribution,
    source_of_material,
    bill_name,
    kit_type,
    sub_type,
    material_type,
    material_sub_category,
    other_material_name,
    purchase_kit_name,
    quantity,
    disaster_type,
    d.created_by as distribution_created_by,
    d.created_date as distribution_created_date
from 
{{ ref('int_activities') }} a 
LEFT JOIN {{ref('int_distributions')}} d 
    ON  a.activity_id=d.activity_id
