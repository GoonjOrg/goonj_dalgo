-- Activities by state and financial year with self/partner breakdown (detail level)
{{ config(
    materialized='table',
    tags=['activities', 'prod', 'salesforce', 'state_analysis', 'detail_level']
) }}

SELECT
    -- Location and time dimensions
    state,
    district,
    block,
    tola_mohalla,
    CASE 
        WHEN EXTRACT(MONTH FROM activity_created_date) >= 4 
        THEN EXTRACT(YEAR FROM activity_created_date)::INTEGER
        ELSE (EXTRACT(YEAR FROM activity_created_date) - 1)::INTEGER
    END AS financial_year,
    
    -- Enhanced partner classification logic using account data
    CASE 
        WHEN account_id IS NULL OR activity_account_name IS NULL OR activity_account_name = '' OR activity_account_name = 'Goonj' OR LOWER(activity_account_name) LIKE '%goonj%' OR LOWER(account_name_from_account_table) LIKE '%goonj%' THEN 'Self'
        ELSE 'Partner'
    END AS organization_type,
    
    -- Account information
    account_id,
    account_type,
    industry,
    activity_account_name,
    account_name_from_account_table,
    
    -- Activity details
    activity_id,
    activity_name,
    type_of_initiative,
    activity_type,
    activity_category,
    activity_sub_type,
    activity_created_date,
    activity_modified_date,
    activity_creator_id,
    activity_owner_id,
    activity_last_modified_by_id,
    
    -- Student participation metrics
    --total_students,
    --_students,
    --female_students,
    --activity_with_students,
    --activity_without_students,
    
    -- Additional participant metrics
    --cfw_participants,
    --njpc_participants,
    --count_of_da,
    --number_of_activities,
    
    -- Quality and compliance metrics
    --form_checked,
    --disclaimer_filled,
    --from_mobile_app,
    
    -- Photo documentation metrics
    has_before_photo,
    has_during_photo,
    has_after_photo,
    before_photo,
    during_photo,
    after_photo,
    s2s_photo_info,
    njpc_photo_info,
    
    -- Additional fields for analysis
    objective_of_cfw_work,
    target_community,
    type_of_educational_entity,
    measurement_type,
    remarks,
    is_education_and_health,
    other_block,
    other_village

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
    CASE 
        WHEN EXTRACT(MONTH FROM activity_created_date) >= 4 
        THEN EXTRACT(YEAR FROM activity_created_date)
        ELSE (EXTRACT(YEAR FROM activity_created_date) - 1)
    END DESC,
    organization_type,
    activity_created_date DESC
