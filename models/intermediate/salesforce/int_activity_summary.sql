-- Activity details by location and initiative (no aggregates)
{{ config(
    materialized='table',
    tags=['activities', 'intermediate', 'salesforce', 'detail_level', 'business_logic']
) }}

SELECT
    -- Location information
    a.state,
    a.district,
    a.block,
    a.other_block,
    a.tola_mohalla,
    a.other,
    
    -- Activity classification
    a."Type_of_Initiative__c" AS type_of_initiative,
    a."Activity_Type__c" AS activity_type,
    a."Activity_Category__c" AS activity_category,
    a."Activity_Sub_Type__c" AS activity_sub_type,
    a."Objective_of_DFW_work__c" AS objective_of_dfw_work,
    a."Target_Community__c" AS target_community,
    a."Type_of_educational_entity__c" AS type_of_educational_entity,
    a."Measurement_Type__c" AS measurement_type,
    
    -- Account information
    a.account_name AS activity_account_name,
    acc.account_id,
    acc.account_name AS account_name_from_account_table,
    acc.account_type,
    acc.industry,
    acc.account_source,
    
    -- System information
    a."Id" AS activity_id,
    a."Name" AS activity_name,
    a."CreatedDate" AS activity_created_date,
    a."LastModifiedDate" AS activity_modified_date,
    a."CreatedById" AS activity_creator_id,
    a."OwnerId" AS activity_owner_id,
    a."LastModifiedById" AS activity_last_modified_by_id,
    
    -- Student participation
    COALESCE(a.male_students, 0)::INTEGER AS male_students,
    COALESCE(a.female_students, 0)::INTEGER AS female_students,
    (COALESCE(a.male_students, 0) + COALESCE(a.female_students, 0))::INTEGER AS total_students,
    CASE WHEN a."Activity_Conducted_With_Students__c" = 'true' OR a."Activity_Conducted_With_Students__c" = 'TRUE' OR a."Activity_Conducted_With_Students__c" = 'True' THEN 1 ELSE 0 END::INTEGER AS activity_with_students,
    CASE WHEN a."Activity_Conducted_With_Students__c" = 'false' OR a."Activity_Conducted_With_Students__c" = 'FALSE' OR a."Activity_Conducted_With_Students__c" = 'False' THEN 1 ELSE 0 END::INTEGER AS activity_without_students,
    
    -- Additional participants
    COALESCE(a.cfw_participants, 0)::INTEGER AS cfw_participants,
    COALESCE(a.njpc_participants, 0)::INTEGER AS njpc_participants,
    COALESCE(a."Count_of_DA__c", 0)::INTEGER AS count_of_da,
    COALESCE(a."Number_of_Activities__c", 0)::INTEGER AS number_of_activities,
    
    -- Quality and compliance
    CASE WHEN a."Form_Cross_Checked__c" = 'true' OR a."Form_Cross_Checked__c" = 'TRUE' OR a."Form_Cross_Checked__c" = 'True' THEN 1 ELSE 0 END::INTEGER AS form_checked,
    CASE WHEN a."Was_Disclaimer_Form_Filled__c" = 'true' OR a."Was_Disclaimer_Form_Filled__c" = 'TRUE' OR a."Was_Disclaimer_Form_Filled__c" = 'True' THEN 1 ELSE 0 END::INTEGER AS disclaimer_filled,
    CASE WHEN a."Is_Created_from_Avni__c" = 'true' OR a."Is_Created_from_Avni__c" = 'TRUE' OR a."Is_Created_from_Avni__c" = 'True' THEN 1 ELSE 0 END::INTEGER AS from_mobile_app,
    
    -- Photo documentation
    CASE WHEN a.before_photo IS NOT NULL THEN 1 ELSE 0 END::INTEGER AS has_before_photo,
    CASE WHEN a.during_photo IS NOT NULL THEN 1 ELSE 0 END::INTEGER AS has_during_photo,
    CASE WHEN a.after_photo IS NOT NULL THEN 1 ELSE 0 END::INTEGER AS has_after_photo,
    a.before_photo,
    a.during_photo,
    a.after_photo,
    a.s2s_photo_info,
    a.njpc_photo_info,
    
    -- Additional information
    a.remarks,
    a."Education_and_Health__c" AS education_and_health,
    a.airbyte_raw_id

FROM {{ ref('staging_activity') }} a
LEFT JOIN {{ ref('staging_account') }} acc 
    ON a.account_name = acc.account_name

ORDER BY 
    a.state,
    a.district,
    a.block,
    a."CreatedDate" DESC,
    a."Type_of_Initiative__c"
