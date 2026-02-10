{{ config(
    materialized='table',
    tags=['kit', 'staging', 'salesforce']
) }}

SELECT
    "Id" as kit_id,
    "Name" as kit_name,
    "OwnerId" as owner_id,
    "Type__c" AS kit_type,
    "IsDeleted" as is_deleted,
    "Remarks__c" AS remarks,
    "CreatedById" as created_by_id,
    "CreatedDate" as created_date,
    "Quantity__c" AS quantity,
    "Kit_Source__c" AS kit_source,
    "Kit_Status__c" AS kit_status,
    "Depreciated__c" AS depreciated,
    "LastViewedDate" as last_viewed_date,
    "SystemModstamp" as system_mod_stamp,
    "Kit_Sub_Type__c" AS kit_sub_type,
    "LastModifiedById" as last_modified_by_id,
    "LastModifiedDate" as last_modified_date,
    "LastReferencedDate" as last_referenced_date,
    "Current_Quantity__c" AS current_quantity,
    "Other_Kit_Detail__c" AS other_kit_detail,
    "Kit_Creation_Date__c" AS kit_creation_date,
    "Original_Quantity__c" AS original_quantity,
    "Processing_Center__c" AS processing_center,
    "Number_of_People_Involved__c" AS number_of_people_involved
FROM {{ source('staging_salesforce', 'kit') }}
WHERE
    -- Don't include deleted records
       "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False