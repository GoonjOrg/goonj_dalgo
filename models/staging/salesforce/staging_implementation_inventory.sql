-- Implementation inventory data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['implementation_inventory', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS implementation_inventory_id,
    "Name" AS implementation_inventory_name,
    "OwnerId" AS owner_id,
    "Unit__c" AS unit,
    "IsDeleted" AS is_deleted,
    "Remarks__c" AS remarks,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "Kit_Type__c" AS kit_type,
    "Sub_type__c" AS sub_type,
    "Bill_Name__c" AS bill_name,
    "Unique_Id__c" AS unique_id,
    "Dispatch_ID__c" AS dispatch_id,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_modstamp,
    "Created_From__c" AS created_from,
    "Vehicle_type__c" AS vehicle_type,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Material_Name__c" AS material_name,
    "Material_Type__c" AS material_type,
    "Original_Name__c" AS original_name,
    "LastReferencedDate" AS last_referenced_date,

    -- Implementation inventory specific fields
    "Material_Kit_Id__c" AS material_kit_id,
    "Current_Quantity__c" AS current_quantity,
    "Date_of_receiving__c" AS date_of_receiving,
    "Material_Kit_Name__c" AS material_kit_name,
    "Original_Quantity__c" AS original_quantity,
    "Purchase_kit_name__c" AS purchase_kit_name,
    "From_which_account__c" AS from_which_account,
    "Source_of_Material__c" AS source_of_material,
    "Center_Field_Office__c" AS center_field_office,
    "Created_or_received__c" AS created_or_received,
    "Other_material_name__c" AS other_material_name,
    "Dispatch_Line_Item_ID__c" AS dispatch_line_item_id,
    "Dispatch_Received_Status__c" AS dispatch_received_status,
    "Center_Field_office_State__c" AS center_field_office_state,
    "Purchased_Created_Received__c" AS purchased_created_received,
    "Center_Field_office_District__c" AS center_field_office_district,
    "Dispatch_Received_Status_Line_Item__c" AS dispatch_received_status_line_item,
    "Implementation_Material_sub_Category__c" AS implementation_material_sub_category

FROM {{ source('staging_salesforce', 'implementation_inventory') }}

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
