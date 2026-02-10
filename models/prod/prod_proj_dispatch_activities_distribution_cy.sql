{{ config(
    materialized='table',
    tags=['proj_dispatch_activities_distribution_cy', 'prod', 'salesforce']
) }}


WITH current_fy AS (
    SELECT 
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4 
            THEN EXTRACT(YEAR FROM CURRENT_DATE)::text || '-' || RIGHT((EXTRACT(YEAR FROM CURRENT_DATE) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM CURRENT_DATE) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM CURRENT_DATE)::text, 2)
        END AS current_financial_year
),

--For the project
filtered_distributions AS (
    SELECT * FROM {{ ref('int_distributions') }}
    WHERE kit_type IS NOT NULL 
       OR material_type IS NOT NULL
),


filtered_dispatches AS (
    SELECT * FROM {{ ref('int_dispatches') }} dispatches
    cross join current_fy cfy
    WHERE 
    dispatches.annual_year=cfy.current_financial_year AND
    dispatches.internal_demand != 'Internal'
     
)


select distinct
    a.annual_year,
    a.month,
    a.monthnum,
    a.quarter,
    a.state,
    a.district,
    a.block,
    a.other_block,
    a.village,
    a.other_village,
    a.account_name,
    a.account_type,
    a.type_of_initiative as activity_type_of_initiative,
    a.created_by,
    a.created_date,
    a.activity_name,
    activity_type,
    activity_category,
    activity_sub_type,
    other_sub_type,
    activity_start_date,
    activity_end_date,
    a.activity_id,
    objective_of_cfw_work,
    other_objective,
    is_education_and_health,
    number_of_activities,
    num_working_days,
    num_cfw_female,
    num_cfw_male,
    num_cfw_others,
    measurement_type,
    length,
    breadth,
    numbers,
    diameter,
    depth_height,
    actvity_conducted_with_students,
    num_s2s_participants,
    num_s2s_days,
    num_njpc_days,
    num_njpc_female,
    num_njpc_male,
    num_njpc_others,
    distribution_id,
    distribution_name,
    distribution_date,
    d.disaster_type,
    d.type_of_initiative as distribution_type_of_initiative,
    d.distribution_line_name,
    dispatches.dispatch_name,
    dispatches.dispatch_id,
    dispatches.dispatch_date,
    dispatches.dispatch_line_item_id,
    dispatches.dispatch_line_item_name,
    d.implementation_inventory_name,
    d.source_of_material,
    d.bill_name,
    d.kit_type,
    d.sub_type,
    d.material_type,
    d.material_sub_category,
    d.other_material_name,
    d.purchase_kit_name,
    d.quantity,
    d.distribution_activity_name,
    d.distribution_activity_id,
    d.created_by as distribution_created_by,
    d.created_date as distribution_created_date
from 
    filtered_dispatches as dispatches left join
    filtered_distributions as d
    on d.dispatch_line_item_id = dispatches.dispatch_line_item_id
    left join
    {{ ref('int_activities') }} a 
   on a.activity_id=d.activity_id

