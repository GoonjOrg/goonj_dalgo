{{ config(
    materialized='table',
    tags=['demands', 'intermediate', 'salesforce']
) }}


SELECT 
    -- Demand information from relief_requirement
    rr.demand_id as demand_id,
    rr.demand_name as demand_name,
    CASE 
            WHEN EXTRACT(MONTH FROM rr.date_of_engagement) >= 4 
            THEN EXTRACT(YEAR FROM rr.date_of_engagement)::text || '-' || RIGHT((EXTRACT(YEAR FROM rr.date_of_engagement) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM rr.date_of_engagement) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM rr.date_of_engagement)::text, 2)
        END AS annual_year,
    CASE 
    WHEN EXTRACT(MONTH FROM rr.date_of_engagement) BETWEEN 4 AND 6 THEN 'Q1'
    WHEN EXTRACT(MONTH FROM rr.date_of_engagement) BETWEEN 7 AND 9 THEN 'Q2'
    WHEN EXTRACT(MONTH FROM rr.date_of_engagement) BETWEEN 10 AND 12 THEN 'Q3'
    WHEN EXTRACT(MONTH FROM rr.date_of_engagement) BETWEEN 1 AND 3 THEN 'Q4'
    END AS quarter,
    TO_CHAR(rr.date_of_engagement, 'Mon') as month, 
    case when account.account_name like '%Goonj%' then 'Self' else 'Partner' end as account_type,    
    rr.date_of_engagement as demand_date,
    rr.created_date as demand_created_date,
    rr.last_modified_date as demand_last_modified_date,
    rr.demand_status,
    rr.disaster_type,
    rr.state,
    rr.no_of_families,
    rr.no_of_individuals,
    rr.name_of_the_org as account_id,
    rr.from_which_processing_center as processing_center,
    rr.internal_demand,
    rr.is_local_demand,
    
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
    rr.created_date,
    rr.last_modified_date,
    dv.created_date as validation_created_date,
    da.created_date as assignment_created_date,
    dpv.created_date as post_validation_created_date,

    -- Account information
    account.account_name as account_name,
    account.account_type as type_of_account
    
FROM {{ ref('staging_relief_requirement') }} rr
LEFT JOIN {{ ref('staging_demand_validation') }}dv ON rr.demand_id = dv.demand  
LEFT JOIN {{ ref('staging_demand_assignment') }} da ON rr.demand_id = da.demand
LEFT JOIN {{ ref('staging_demand_post_validation') }} dpv ON rr.demand_id = dpv.demand 
LEFT JOIN {{ref('staging_account')}} account on rr.name_of_the_org = account.account_id

where rr.is_deleted=False