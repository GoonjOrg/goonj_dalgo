{{ config(
    materialized='table',
    tags=['dispatch_line_items', 'staging', 'salesforce']
) }}

SELECT
    "Id" as dispatch_line_item_id,
    "Name",
    "Kit__c" AS kit_id,
    "OwnerId",
    "Unit__c" AS unit,
    "IsDeleted",
    "Others__c" AS others,
    "CreatedById",
    "CreatedDate",
    "Quantity__c" AS quantity,
    "RecordTypeId",
    "LastViewedDate",
    "SystemModstamp",
    "LastActivityDate",
    "LastModifiedById",
    "LastModifiedDate",
    "Material_Code__c" AS material_code,
    "Material_Type__c" AS material_type,
    "Others_Ration__c" AS others_ration,
    "Others_General__c" AS others_general,
    "Others_Hygiene__c" AS others_hygiene,
    "Dispatch_Status__c" AS dispatch_status,
    "LastReferencedDate",
    "Contributed_Item__c" AS contributed_item,
    "Material_Content__c" AS material_content,
    "Dispatch_Status_Id__c" AS dispatch_status_id,
    "Material_Inventory__c" AS material_inventory_id,
    "Store_Item_Category__c" AS store_item_category,
    "Cooked_food_No_of_meals__c" AS cooked_food_no_of_meals
FROM {{ source('staging_salesforce', 'dispatch_line_items') }}

WHERE
    -- Don't include deleted records
    "IsDeleted" = FALSE
    
    -- Make sure we have the basic information we need
    AND "Id" IS NOT NULL
    AND "CreatedDate" IS NOT NULL

ORDER BY "CreatedDate" DESC, "Id"