-- Activity summary by location and initiative
{{ config(
    materialized='table',
    tags=['activities', 'intermediate', 'salesforce', 'aggregation', 'business_logic']
) }}

SELECT
    state,
    district,
    block,
    "Type_of_Initiative__c" AS type_of_initiative,
    account_name,
    MIN("CreatedDate") AS activity_start_date,
    MAX("CreatedDate") AS activity_end_date,
    COUNT(*) AS no_of_activities,
    COUNT(DISTINCT "Id") AS unique_activity_count,
    SUM(COALESCE(male_students, 0)) AS total_male_students,
    SUM(COALESCE(female_students, 0)) AS total_female_students,
    SUM(COALESCE(male_students, 0) + COALESCE(female_students, 0)) AS total_students,
    COUNT(CASE WHEN "Activity_Conducted_With_Students__c" = 'true' OR "Activity_Conducted_With_Students__c" = 'TRUE' OR "Activity_Conducted_With_Students__c" = 'True' THEN 1 END) AS activities_with_students,
    COUNT(CASE WHEN "Activity_Conducted_With_Students__c" = 'false' OR "Activity_Conducted_With_Students__c" = 'FALSE' OR "Activity_Conducted_With_Students__c" = 'False' THEN 1 END) AS activities_without_students,
    SUM(COALESCE(cfw_participants, 0)) AS total_cfw_participants,
    SUM(COALESCE(njpc_participants, 0)) AS total_njpc_participants,
    COUNT(CASE WHEN "Form_Cross_Checked__c" = 'true' OR "Form_Cross_Checked__c" = 'TRUE' OR "Form_Cross_Checked__c" = 'True' THEN 1 END) AS activities_form_checked,
    COUNT(CASE WHEN "Was_Disclaimer_Form_Filled__c" = 'true' OR "Was_Disclaimer_Form_Filled__c" = 'TRUE' OR "Was_Disclaimer_Form_Filled__c" = 'True' THEN 1 END) AS activities_disclaimer_filled,
    COUNT(CASE WHEN "Is_Created_from_Avni__c" = 'true' OR "Is_Created_from_Avni__c" = 'TRUE' OR "Is_Created_from_Avni__c" = 'True' THEN 1 END) AS activities_from_mobile_app,
    COUNT(CASE WHEN before_photo IS NOT NULL THEN 1 END) AS activities_with_before_photos,
    COUNT(CASE WHEN during_photo IS NOT NULL THEN 1 END) AS activities_with_during_photos,
    COUNT(CASE WHEN after_photo IS NOT NULL THEN 1 END) AS activities_with_after_photos,
    MIN("CreatedDate") AS first_activity_created,
    MAX("LastModifiedDate") AS last_activity_modified,
    COUNT(DISTINCT "CreatedById") AS unique_creators

FROM {{ ref('staging_activity') }}

GROUP BY 
    state,
    district, 
    block,
    "Type_of_Initiative__c",
    account_name

ORDER BY 
    state,
    district,
    block,
    no_of_activities DESC,
    type_of_initiative
