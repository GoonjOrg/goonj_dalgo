-- Activity summary by location and initiative
{{ config(
    materialized='table',
    tags=['activities', 'intermediate', 'salesforce', 'aggregation', 'business_logic']
) }}

SELECT
    a.state,
    a.district,
    a.block,
    a."Type_of_Initiative__c" AS type_of_initiative,
    a.account_name,
    acc.account_id,
    acc.account_name AS account_name_from_account_table,
    acc.account_type,
    acc.industry,
    MIN(a."CreatedDate") AS activity_start_date,
    MAX(a."CreatedDate") AS activity_end_date,
    COUNT(*) AS no_of_activities,
    COUNT(DISTINCT a."Id") AS unique_activity_count,
    SUM(COALESCE(a.male_students, 0)) AS total_male_students,
    SUM(COALESCE(a.female_students, 0)) AS total_female_students,
    SUM(COALESCE(a.male_students, 0) + COALESCE(a.female_students, 0)) AS total_students,
    COUNT(CASE WHEN a."Activity_Conducted_With_Students__c" = 'true' OR a."Activity_Conducted_With_Students__c" = 'TRUE' OR a."Activity_Conducted_With_Students__c" = 'True' THEN 1 END) AS activities_with_students,
    COUNT(CASE WHEN a."Activity_Conducted_With_Students__c" = 'false' OR a."Activity_Conducted_With_Students__c" = 'FALSE' OR a."Activity_Conducted_With_Students__c" = 'False' THEN 1 END) AS activities_without_students,
    SUM(COALESCE(a.cfw_participants, 0)) AS total_cfw_participants,
    SUM(COALESCE(a.njpc_participants, 0)) AS total_njpc_participants,
    COUNT(CASE WHEN a."Form_Cross_Checked__c" = 'true' OR a."Form_Cross_Checked__c" = 'TRUE' OR a."Form_Cross_Checked__c" = 'True' THEN 1 END) AS activities_form_checked,
    COUNT(CASE WHEN a."Was_Disclaimer_Form_Filled__c" = 'true' OR a."Was_Disclaimer_Form_Filled__c" = 'TRUE' OR a."Was_Disclaimer_Form_Filled__c" = 'True' THEN 1 END) AS activities_disclaimer_filled,
    COUNT(CASE WHEN a."Is_Created_from_Avni__c" = 'true' OR a."Is_Created_from_Avni__c" = 'TRUE' OR a."Is_Created_from_Avni__c" = 'True' THEN 1 END) AS activities_from_mobile_app,
    COUNT(CASE WHEN a.before_photo IS NOT NULL THEN 1 END) AS activities_with_before_photos,
    COUNT(CASE WHEN a.during_photo IS NOT NULL THEN 1 END) AS activities_with_during_photos,
    COUNT(CASE WHEN a.after_photo IS NOT NULL THEN 1 END) AS activities_with_after_photos,
    MIN(a."CreatedDate") AS first_activity_created,
    MAX(a."LastModifiedDate") AS last_activity_modified,
    COUNT(DISTINCT a."CreatedById") AS unique_creators

FROM {{ ref('staging_activity') }} a
LEFT JOIN {{ ref('staging_account') }} acc 
    ON a.account_name = acc.account_name

GROUP BY 
    a.state,
    a.district, 
    a.block,
    a."Type_of_Initiative__c",
    a.account_name,
    acc.account_id,
    acc.account_name,
    acc.account_type,
    acc.industry

ORDER BY 
    a.state,
    a.district,
    a.block,
    no_of_activities DESC,
    a."Type_of_Initiative__c"
