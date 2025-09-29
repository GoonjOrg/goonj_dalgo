-- Demand assignment data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['demand_assignment', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS demand_assignment_id,
    "Name" AS demand_assignment_name,
    "Demand__c" AS demand,
    "IsDeleted" AS is_deleted,
    "Status__c" AS status,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Assigned_by__c" AS assigned_by,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_modstamp,
    "Local_Demand__c" AS local_demand,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Internal_Demand__c" AS internal_demand,
    "LastReferencedDate" AS last_referenced_date,
    "Name_of_Account__c" AS name_of_account,
    "Purchase_material__c" AS purchase_material,
    "From_which_processing_Center__c" AS from_which_processing_center,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'demand_assignment') }}

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
