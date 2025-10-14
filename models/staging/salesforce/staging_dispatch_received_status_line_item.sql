-- Dispatch Received Status Line Item data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['dispatch_received_status_line_item', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    "Id" AS dispatch_received_status_line_item_id,
    "Name" AS dispatch_received_status_line_item_name,
    "Unit__c" AS unit_c,
    "IsDeleted" AS is_deleted,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Item_Name__c" AS item_name_c,
    "SystemModstamp" AS system_modstamp,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Receiving_Status__c" AS receiving_status_c,
    "Received_Quantity__c" AS received_quantity_c,
    "Dispatched_Quantity__c" AS dispatched_quantity_c,
    "Is_Created_from_Avni__c" AS is_created_from_avni_c,
    "Dispatch_Received_Status__c" AS dispatch_received_status_c,
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'dispatch_received_status_line_item') }}

WHERE
    -- Don't include deleted records
    "IsDeleted" = FALSE
    
    -- Make sure we have the basic information we need
    AND "Id" IS NOT NULL
    AND "CreatedDate" IS NOT NULL

ORDER BY "CreatedDate" DESC, "Id"
