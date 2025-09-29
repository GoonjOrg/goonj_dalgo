-- Demand post validation data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['demand_post_validation', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS demand_post_validation_id,
    "Name" AS demand_post_validation_name,
    "OwnerId" AS owner_id,
    "Demand__c" AS demand,
    "IsDeleted" AS is_deleted,
    "Status__c" AS status,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Duplicate__c" AS duplicate,
    "Validation__c" AS validation,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_modstamp,
    "Local_Demand__c" AS local_demand,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Demands_Closed__c" AS demands_closed,
    "Dispatch_Stage__c" AS dispatch_stage,
    "Internal_Demand__c" AS internal_demand,
    "LastReferencedDate" AS last_referenced_date,
    "Name_of_Account__c" AS name_of_account,
    "Demand_Validation__c" AS demand_validation,
    "Type_of_Initiative__c" AS type_of_initiative,
    "Under_Process_Date__c" AS under_process_date,

    -- Additional custom fields
    "From_which_processing__c" AS from_which_processing,
    "Reason_for_postponing__c" AS reason_for_postponing,
    "Reason_for_closing_demand__c" AS reason_for_closing_demand,
    "Tentative_date_of_dispatch__c" AS tentative_date_of_dispatch,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'demand_post_validation') }}

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
