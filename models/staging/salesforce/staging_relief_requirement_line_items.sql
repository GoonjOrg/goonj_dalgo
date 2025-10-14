-- Relief requirement line items data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['relief_requirement_line_items', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS demand_line_id,
    "Name" AS demand_line_name,
    "OwnerId" AS owner_id,
    "Unit__c" AS unit,
    "Demand__c" AS demand,
    "IsDeleted" AS is_deleted,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Kit_Types__c" AS kit_types,
    "RecordTypeId" AS record_type_id,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_modstamp,
    "Kit_Sub_Type__c" AS kit_sub_type,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Total_Quantity__c" AS total_quantity,
    "LastReferencedDate" AS last_referenced_date,
    "Non_Kit_Material__c" AS non_kit_material,
    "Other_Kit_Details__c" AS other_kit_details,
    "Relief_Requirement__c" AS relief_requirement,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'relief_requirement_line_items') }}

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
