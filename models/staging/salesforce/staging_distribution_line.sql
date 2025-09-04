{{ config(
    materialized='table',
    tags=['distribution_line', 'staging', 'salesforce']
) }}

SELECT
    "Id" AS distribution_line_id,
    "Name",
    "OwnerId",
    "Unit__c" AS unit,
    "IsDeleted",
    "CreatedById",
    "CreatedDate",
    "Quantity__c" AS quantity,
    "RecordTypeId",
    "LastViewedDate",
    "SystemModstamp",
    "Distribution__c" AS distribution_id,
    "Dispatched_To__c" AS dispatched_to,
    "LastActivityDate",
    "LastModifiedById",
    "LastModifiedDate",
    "LastReferencedDate",
    "Is_Created_from_Avni__c" AS is_created_from_avni,
    "Implementation_Inventory__c" AS implementation_inventory,
    "Avni_Implementation_Inventory__c" AS avni_implementation_inventory,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
FROM {{ source('staging_salesforce', 'distribution_line') }}