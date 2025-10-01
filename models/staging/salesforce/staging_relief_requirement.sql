-- Relief requirement data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['relief_requirement', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS relief_requirement_id,
    "Name" AS relief_requirement_name,
    "Name__c" AS name_custom,
    "OwnerId" AS owner_id,
    "Type__c" AS type,
    "DM_ID__c" AS dm_id,
    "State__c" AS state,
    "IsDeleted" AS is_deleted,
    "Office__c" AS office,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "LastViewedDate" AS last_viewed_date,
    "Record_Type__c" AS record_type,
    "SystemModstamp" AS system_modstamp,
    "Demand_State__c" AS demand_state,
    "No_of_People__c" AS no_of_people,
    "Target_Group__c" AS target_group,
    "learncab1234__c" AS learncab1234,
    "Demand_Status__c" AS demand_status,
    "Disaster_Type__c" AS disaster_type,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Contact_Number__c" AS contact_number,
    "No_Of_Families__c" AS no_of_families,
    "Source_of_Info__c" AS source_of_info,
    "Contact_details__c" AS contact_details,
    "Internal_demand__c" AS internal_demand,
    "Is_Local_Demand__c" AS is_local_demand,
    "LastReferencedDate" AS last_referenced_date,
    "Name_Of_The_Org__c" AS name_of_the_org,
    "Any_Other_Remark__c" AS any_other_remark,
    "Dispatch_Address__c" AS dispatch_address,
    "No_Of_Individuals__c" AS no_of_individuals,
    "Reference_of_Team__c" AS reference_of_team,
    "Status_Of_The_Org__c" AS status_of_the_org,
    "Type_of_Community__c" AS type_of_community,
    "Date_of_engagement__c" AS date_of_engagement,
    "Name_of_the_Person__c" AS name_of_the_person,
    "Reason_for_urgency__c" AS reason_for_urgency,
    "Type_of_Initiative__c" AS type_of_initiative,
    "Coordinating_Office__c" AS coordinating_office,
    "Reason_for_Rejection__c" AS reason_for_rejection,
    "Reference_Team_Others__c" AS reference_team_others,
    "other_Internal_purpose__c" AS other_internal_purpose,
    "Detail_of_Pending_Reports__c" AS detail_of_pending_reports,
    "Name_And_Contact_No_Of_POC__c" AS name_and_contact_no_of_poc,
    "Account_Details_If_Available__c" AS account_details_if_available,
    "From_which_Processing_Center__c" AS from_which_processing_center,
    "Submit_Report_of_Last_Dispatch__c" AS submit_report_of_last_dispatch,
    "What_kind_of_support_do_you_need__c" AS what_kind_of_support_do_you_need,
    "Till_when_is_the_material_required__c" AS till_when_is_the_material_required,
    "Action_taken_or_status_of_the_request__c" AS action_taken_or_status_of_the_request,
    "Submitted_Reports_to_Head_or_Regional__c" AS submitted_reports_to_head_or_regional,
    "Whether_material_is_on_an_urgent_basis__c" AS whether_material_is_on_an_urgent_basis,
    "Local_Permission_Available_With_The_Org__c" AS local_permission_available_with_the_org,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'relief_requirement') }}

WHERE
    -- Don't include deleted records
    "IsDeleted" = FALSE
    
    -- Make sure we have the basic information we need
    AND "Id" IS NOT NULL
    AND "CreatedDate" IS NOT NULL
    
    -- Don't include completely empty or invalid records
    AND "Name" IS NOT NULL
    AND "Name" != ''

ORDER BY "CreatedDate" DESC, "Id"
