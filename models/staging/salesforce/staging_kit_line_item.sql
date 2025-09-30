-- Kit line item data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['kit_line_item', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS kit_line_item_id,
    "Name" AS kit_line_item_name,
    "OwnerId" AS owner_id,
    "IsDeleted" AS is_deleted,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "RecordTypeId" AS record_type_id,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_modstamp,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "LastReferencedDate" AS last_referenced_date,

    -- Kit line item specific fields
    "Kit__c" AS kit,
    "Unit__c" AS unit,
    "Quantity__c" AS quantity,
    "Contributed_Item__c" AS contributed_item,
    "Implementation_Inventory__c" AS implementation_inventory,
    "Purchase_High_Value_Material__c" AS purchase_high_value_material

FROM {{ source('staging_salesforce', 'kit_line_item') }}

WHERE
    -- Don't include deleted records
    "IsDeleted" = FALSE
    
    -- Make sure we have the basic information we need
    AND "Id" IS NOT NULL
    AND "CreatedDate" IS NOT NULL

ORDER BY "CreatedDate" DESC, "Id"
