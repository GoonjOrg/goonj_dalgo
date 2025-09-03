-- Activities by state and financial year with self/partner breakdown (detail level)
{{ config(
    materialized='table',
    tags=['activities', 'prod', 'salesforce', 'state_analysis', 'detail_level']
) }}

SELECT
    state,
    CASE 
        WHEN EXTRACT(MONTH FROM activity_created_date) >= 4 
        THEN EXTRACT(YEAR FROM activity_created_date)::INTEGER
        ELSE (EXTRACT(YEAR FROM activity_created_date) - 1)::INTEGER
    END AS financial_year,
    
    -- Enhanced partner classification logic using account data
    CASE 
        WHEN account_id IS NULL OR account_name IS NULL OR account_name = '' OR account_name = 'Goonj' OR LOWER(account_name) LIKE '%goonj%' OR LOWER(account_name_from_account_table) LIKE '%goonj%' THEN 'Self'
        ELSE 'Partner'
    END AS organization_type,
    
    -- Account information (retained as individual fields)
    account_id,
    account_type,
    industry,
    account_name,
    account_name_from_account_table,
    
    -- Activity details (retained as individual fields)
    activity_id,
    district,
    block,
    type_of_initiative,
    activity_created_date,
    activity_modified_date,
    activity_creator_id,
    
    -- Student participation metrics (retained as individual fields)
    total_students,
    male_students,
    female_students,
    activity_with_students,
    
    -- Additional participant metrics (retained as individual fields)
    cfw_participants,
    njpc_participants,
    
    -- Quality and compliance metrics (retained as individual fields)
    form_checked,
    disclaimer_filled,
    from_mobile_app,
    
    -- Photo documentation metrics (retained as individual fields)
    has_before_photo,
    has_during_photo,
    has_after_photo

FROM {{ ref('int_activity_summary') }}

WHERE 
    state IS NOT NULL 
    AND activity_created_date IS NOT NULL
    AND CASE 
        WHEN EXTRACT(MONTH FROM activity_created_date) >= 4 
        THEN EXTRACT(YEAR FROM activity_created_date)
        ELSE (EXTRACT(YEAR FROM activity_created_date) - 1)
    END >= 2020

ORDER BY 
    state,
    financial_year DESC,
    organization_type,
    activity_created_date DESC
