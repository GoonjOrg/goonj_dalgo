{{ config(
    materialized='table',
    tags=['demands', 'intermediate', 'salesforce']
) }}


WITH base_dates AS (
    SELECT 
        *,
        EXTRACT(MONTH FROM date_of_engagement) as m_num,
        EXTRACT(YEAR FROM date_of_engagement) as y_num,
        TO_CHAR(date_of_engagement, 'Mon') as month_name
    FROM {{ ref('staging_relief_requirement') }}
),
demand_calculated_dates AS (
    SELECT 
        *,
        CASE 
            WHEN m_num >= 4 THEN y_num::text || '-' || RIGHT((y_num + 1)::text, 2)
            ELSE (y_num - 1)::text || '-' || RIGHT(y_num::text, 2)
        END AS annual_year,
        CASE 
            WHEN m_num BETWEEN 4 AND 6 THEN 'Q1'
            WHEN m_num BETWEEN 7 AND 9 THEN 'Q2'
            WHEN m_num BETWEEN 10 AND 12 THEN 'Q3'
            WHEN m_num BETWEEN 1 AND 3 THEN 'Q4'
        END AS quarter
    FROM base_dates
)

SELECT 
    -- Demand information from relief_requirement
    dcd.demand_id as demand_id,
    dcd.demand_name as demand_name,
    dcd.annual_year,
    dcd.quarter,
    dcd.month_name as month,
    dcd.m_num as monthnum,
    
    case when account.account_name like '%Goonj%' then 'Self' else 'Partner' end as account_type,    
    dcd.date_of_engagement as demand_date,
    dcd.created_date as demand_created_date,
    dcd.last_modified_date as demand_last_modified_date,
    dcd.demand_status,
    dcd.disaster_type,
    dcd.state,
    dcd.no_of_families,
    dcd.no_of_individuals,
    dcd.name_of_the_org as account_id,
    dcd.from_which_processing_center as processing_center,
    CASE dcd.internal_demand WHEN 'Yes' THEN 'Internal' ELSE 'External' END as internal_demand,
    CASE dcd.is_local_demand WHEN 'Yes' THEN 'Local' ELSE 'Non-Local' END as local_demand,
    
    -- Demand validation information
    dv.status as validation_status,
    dv.validation_reason,
    dv.other_reason as validation_other_reason,
    dv.coordinating_office,
    dv.office as validation_office,
    
    -- Demand assignment information
    da.status as assignment_status,
    da.assigned_by as assignment_assigned_by,
    da.from_which_processing_center as assigned_processing_center,
    
    -- Post-validation information
    dpv.status as post_validation_status,
    dpv.dispatch_stage,
    dpv.reason_for_closing_demand,
    dpv.tentative_date_of_dispatch,
    dpv.type_of_initiative,
    
    -- Timestamps
    dcd.created_date,
    dcd.last_modified_date,
    dv.created_date as validation_created_date,
    da.created_date as assignment_created_date,
    dpv.created_date as post_validation_created_date,

    -- Account information
    account.account_name as account_name,
    account.account_type as type_of_account
    
FROM demand_calculated_dates dcd
LEFT JOIN {{ ref('staging_demand_validation') }}dv ON dcd.demand_id = dv.demand  
LEFT JOIN {{ ref('staging_demand_assignment') }} da ON dcd.demand_id = da.demand
LEFT JOIN {{ ref('staging_demand_post_validation') }} dpv ON dcd.demand_id = dpv.demand 
LEFT JOIN {{ref('staging_account')}} account on dcd.name_of_the_org = account.account_id