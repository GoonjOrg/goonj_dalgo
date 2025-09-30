-- Material inventory data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['material_inventory', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS material_inventory_id,
    "Name" AS material_name,
    "OwnerId" AS owner_id,
    "Other__c" AS other,
    "Store__c" AS store,
    "Active__c" AS active,
    "IsDeleted" AS is_deleted,
    "Company__c" AS company,
    "Remarks__c" AS remarks,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Quantity__c" AS quantity,
    "Sub_Area__c" AS sub_area,
    "Item_Name__c" AS item_name,
    "RecordTypeId" AS record_type_id,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_modstamp,
    "Company_Name__c" AS company_name,
    "Bulk_Material__c" AS bulk_material,
    "Dump_Material__c" AS dump_material,
    "Item_Category__c" AS item_category,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "LastReferencedDate" AS last_referenced_date,

    -- Material inventory specific fields
    "Type_of_Material__c" AS type_of_material,
    "Vehicle_Category__c" AS vehicle_category,
    "Item_Sub_Category__c" AS item_sub_category,
    "Maximum_Threshold__c" AS maximum_threshold,
    "Minimum_Threshold__c" AS minimum_threshold,
    "Processing_Center__c" AS processing_center,
    "Purchased_Item_Name__c" AS purchased_item_name,
    "Unit_of_Measurement__c" AS unit_of_measurement,
    "Raw_Material_Category__c" AS raw_material_category,
    "Raw_Unit_of_Measurement__c" AS raw_unit_of_measurement,
    "Color_code_for_Raw_Material__c" AS color_code_for_raw_material,
    "Sum_of_material_quantity_history__c" AS sum_of_material_quantity_history,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'material_inventory') }}

WHERE
    -- Don't include deleted records
    "IsDeleted" = FALSE
    
    -- Make sure we have the basic information we need
    AND "Id" IS NOT NULL
    AND "CreatedDate" IS NOT NULL
    
    -- Don't include completely empty or invalid records
    AND "Name" IS NOT NULL
    AND "Name" != ''

ORDER BY "LastModifiedDate" DESC, "Id"
