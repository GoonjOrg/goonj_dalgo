-- Material received status data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['material_received_status', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS material_received_status_id,
    "Name" AS material_received_status_name,
    "OwnerId" AS owner_id,
    "IsDeleted" AS is_deleted,
    "Remarks__c" AS remarks,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Source_Id__c" AS source_id,
    "Created_By__c" AS created_by,
    "Modified_By__c" AS modified_by,
    "SystemModstamp" AS system_modstamp,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Received_Date__c" AS received_date,
    "Dispatch_Status__c" AS dispatch_id,
    "Is_Partial_Received__c" AS is_partial_received,
    "Is_Created_from_Avni__c" AS is_created_from_avni,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'material_received_status') }}

WHERE
    -- Don't include deleted records
       "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False