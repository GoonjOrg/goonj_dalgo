{{ config(
    materialized='table',
    tags=['demands', 'prod', 'salesforce']
) }}

SELECT 
    -- Demand information from relief_requirement
    demand_id,
    demand_name,
    annual_year,
    quarter,
    month,
    monthnum,
    receiver_account_type as account_type,    
    demand_date,
    demand_created_date,
    demand_last_modified_date,
    demand_status,
    disaster_type,
    receiver_state as state,
    no_of_families,
    no_of_individuals,
    account_id,
    processing_center_name,
    internal_demand,
    local_demand,
    
    -- Demand validation information
    validation_status,
    validation_reason,
    validation_other_reason,
    coordinating_office,
    validation_office,
    
    -- Demand assignment information
    assignment_status,
    
    -- Post-validation information
    dpv_status,
    dispatch_stage,
    reason_for_closing_demand,
    tentative_date_of_dispatch,
    type_of_initiative,
    
    -- Timestamps   
    created_date,
    last_modified_date,
    validation_created_date,
    assignment_created_date,
    post_validation_created_date,

    -- Account information
    receiver_center_name as account_name,
    receiver_center_type as type_of_account
    
FROM  
{{ ref('int_demands') }} as demands 
--left JOIN {{ ref('int_dispatches') }} as dispatches on dispatches.demand_id = demands.demand_id
--where demands.demand_status!='Closed' and demands.dpv_status!='Closed'

