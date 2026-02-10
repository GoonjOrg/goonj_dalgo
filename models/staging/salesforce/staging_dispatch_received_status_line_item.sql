-- Dispatch Received Status Line Item data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['dispatch_received_status_line_item', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    "Id" AS dispatch_received_status_line_item_id,
    "Name" AS dispatch_received_status_line_item_name,
    "Unit__c" AS unit,
    "IsDeleted" AS is_deleted,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Item_Name__c" AS item_name,
    "SystemModstamp" AS system_modstamp,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Receiving_Status__c" AS receiving_status,
    "Received_Quantity__c" AS received_quantity,
    "Dispatched_Quantity__c" AS dispatched_quantity,
    "Is_Created_from_Avni__c" AS is_created_from_avni,
    "Dispatch_Received_Status__c" AS dispatch_received_status_id,
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'dispatch_received_status_line_item') }}

WHERE
    -- Don't include deleted records
       "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False