{{ config(
    materialized='table',
    tags=['dispatch_line_items', 'staging', 'salesforce']
) }}

SELECT
    "Id" as dispatch_line_item_id,
    "Name" as dispatch_line_item_name,
    "Kit__c" AS kit_id,
    "OwnerId" AS owner_id,
    "Unit__c" AS unit,
    "IsDeleted" AS is_deleted,
    "Others__c" AS others,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Quantity__c" AS quantity,
    "RecordTypeId" AS record_type_id,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_mod_stamp,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Material_Code__c" AS material_code,
    "Material_Type__c" AS material_type,
    "Others_Ration__c" AS others_ration,
    "Others_General__c" AS others_general,
    "Others_Hygiene__c" AS others_hygiene,
    "Dispatch_Status__c" AS dispatch_status,
    "LastReferencedDate" AS last_referenced_date,
    "Contributed_Item__c" AS contributed_item,
    "Material_Content__c" AS material_content,
    "Dispatch_Status_Id__c" AS dispatch_status_id,
    "Material_Inventory__c" AS material_inventory_id,
    "Store_Item_Category__c" AS store_item_category,
    "Cooked_food_No_of_meals__c" AS cooked_food_no_of_meals
FROM {{ source('staging_salesforce', 'dispatch_line_items') }}

WHERE
    -- Don't include deleted records
       "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False