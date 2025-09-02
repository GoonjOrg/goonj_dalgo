-- Activities by state and year with self/partner breakdown
{{ config(
    materialized='table',
    tags=['activities', 'prod', 'salesforce', 'state_analysis', 'yearly_reporting']
) }}

SELECT
    state,
    EXTRACT(YEAR FROM activity_start_date) AS activity_year,
    
    -- Enhanced partner classification logic using account data
    CASE 
        WHEN account_id IS NULL OR account_name IS NULL OR account_name = '' OR account_name = 'Goonj' OR LOWER(account_name) LIKE '%goonj%' OR LOWER(account_name_from_account_table) LIKE '%goonj%' THEN 'Self'
        ELSE 'Partner'
    END AS organization_type,
    
    -- Account information
    COUNT(DISTINCT account_id) AS unique_accounts,
    COUNT(DISTINCT account_type) AS unique_account_types,
    COUNT(DISTINCT industry) AS unique_industries,
    STRING_AGG(DISTINCT account_name, ', ') AS account_names,
    
    -- Activity metrics
    SUM(no_of_activities) AS total_activities,
    COUNT(DISTINCT CONCAT(state, district, block)) AS unique_locations,
    COUNT(DISTINCT type_of_initiative) AS unique_initiatives,
    
    -- Student participation metrics
    SUM(total_students) AS total_students_reached,
    SUM(total_male_students) AS total_male_students,
    SUM(total_female_students) AS total_female_students,
    SUM(activities_with_students) AS activities_with_students,
    
    -- Additional participant metrics
    SUM(total_cfw_participants) AS total_cfw_participants,
    SUM(total_njpc_participants) AS total_njpc_participants,
    
    -- Quality and compliance metrics
    SUM(activities_form_checked) AS activities_form_checked,
    SUM(activities_disclaimer_filled) AS activities_disclaimer_filled,
    SUM(activities_from_mobile_app) AS activities_from_mobile_app,
    
    -- Photo documentation metrics
    SUM(activities_with_before_photos) AS activities_with_before_photos,
    SUM(activities_with_during_photos) AS activities_with_during_photos,
    SUM(activities_with_after_photos) AS activities_with_after_photos,
    
    -- Date range information
    MIN(activity_start_date) AS earliest_activity_date,
    MAX(activity_end_date) AS latest_activity_date,
    
    -- Calculated metrics
    ROUND(AVG(total_students), 2) AS avg_students_per_activity,
    ROUND(SUM(activities_with_students) * 100.0 / SUM(no_of_activities), 2) AS student_participation_rate,
    ROUND(SUM(activities_form_checked) * 100.0 / SUM(no_of_activities), 2) AS form_compliance_rate

FROM {{ ref('int_activity_summary') }}

WHERE 
    state IS NOT NULL 
    AND activity_start_date IS NOT NULL
    AND EXTRACT(YEAR FROM activity_start_date) >= 2020

GROUP BY 
    state,
    EXTRACT(YEAR FROM activity_start_date),
    CASE 
        WHEN account_id IS NULL OR account_name IS NULL OR account_name = '' OR account_name = 'Goonj' OR LOWER(account_name) LIKE '%goonj%' OR LOWER(account_name_from_account_table) LIKE '%goonj%' THEN 'Self'
        ELSE 'Partner'
    END

ORDER BY 
    state,
    activity_year DESC,
    organization_type
