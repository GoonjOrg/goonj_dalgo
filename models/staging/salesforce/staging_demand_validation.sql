-- Demand validation data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['demand_validation', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS demand_validation_id,
    "Name" AS demand_validation_name,
    "OwnerId" AS owner_id,
    "Block__c" AS block,
    "State__c" AS state,
    "Demand__c" AS demand,
    "IsDeleted" AS is_deleted,
    "Office__c" AS office,
    "Status__c" AS status,
    "Street__c" AS street,
    "By_When__c" AS by_when,
    "Village__c" AS village,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "District__c" AS district,
    "Approved_By__c" AS approved_by,
    "LastViewedDate" AS last_viewed_date,
    "Postal_Code__c" AS postal_code,
    "Record_Type__c" AS record_type,
    "SystemModstamp" AS system_modstamp,
    "Other_Reason__c" AS other_reason,
    "Approved_By_2__c" AS approved_by_2,
    "Approved_By_3__c" AS approved_by_3,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Demand_Remarks__c" AS demand_remarks,
    "Internal_Demand__c" AS internal_demand,
    "Is_Local_Demand__c" AS is_local_demand,
    "LastReferencedDate" AS last_referenced_date,
    "Name_of_Account__c" AS name_of_account,
    "Demand_Assignment__c" AS demand_assignment,
    "Validation_Reason__c" AS validation_reason,
    "Coordinating_Office__c" AS coordinating_office,
    "From_which_processing_unit__c" AS from_which_processing_unit,
    "from_which_processing_center__c" AS from_which_processing_center,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'demand_validation') }}

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
