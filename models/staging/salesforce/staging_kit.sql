{{ config(
    materialized='table',
    tags=['kit', 'staging', 'salesforce']
) }}

SELECT
    "Id",
    "Name",
    "OwnerId",
    "Type__c" AS type,
    "IsDeleted",
    "Remarks__c" AS remarks,
    "CreatedById",
    "CreatedDate",
    "Quantity__c" AS quantity,
    "Kit_Source__c" AS kit_source,
    "Kit_Status__c" AS kit_status,
    "Depreciated__c" AS depreciated,
    "LastViewedDate",
    "SystemModstamp",
    "Kit_Sub_Type__c" AS kit_sub_type,
    "LastModifiedById",
    "LastModifiedDate",
    "LastReferencedDate",
    "Current_Quantity__c" AS current_quantity,
    "Other_Kit_Detail__c" AS other_kit_detail,
    "Kit_Creation_Date__c" AS kit_creation_date,
    "Original_Quantity__c" AS original_quantity,
    "Processing_Center__c" AS processing_center,
    "Number_of_People_Involved__c" AS number_of_people_involved
FROM {{ source('staging_salesforce', 'kit') }}
