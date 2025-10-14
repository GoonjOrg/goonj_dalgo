{{ config(
    materialized='table',
    tags=['distribution_line', 'staging', 'salesforce']
) }}

SELECT
    "Id" AS distribution_line_id,
    "Name" AS distribution_line_name,
    "Unit__c" AS unit,
    "Quantity__c" AS quantity,
    "Dispatched_To__c" AS distributed_to,
    "Distribution__c" AS distribution_id,
    "Implementation_Inventory__c" AS implementation_inventory_id,

    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Is_Created_from_Avni__c" AS is_created_from_avni,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS Last_modified_Date,

    "OwnerId" AS owner_id,
    "IsDeleted" AS is_deleted,
    "RecordTypeId" AS record_type_id,
    "SystemModstamp" AS system_mod_stamp,
    "LastActivityDate" AS last_activity_date,
    "LastReferencedDate" AS last_referenced_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta"
FROM {{ source('staging_salesforce', 'distribution_line') }}
