{{ config(
    materialized='table',
    tags=['kit_dispatch', 'intermediate', 'salesforce']
) }}

SELECT 
    -- Kit information
    k.kit_id,
    k.kit_name,
    k.kit_type,
    k.kit_status,
    k.kit_sub_type,
    k.current_quantity as kit_current_quantity,
    k.original_quantity as kit_original_quantity,
    k.processing_center,
    
    -- Dispatch line item information  
    dli.dispatch_line_item_id,
    dli.dispatch_line_item_name,
    dli.quantity as dispatch_quantity,
    dli.unit,
    dli.material_code,
    dli.material_type,
    dli.material_content,
    dli.store_item_category,
    dli.contributed_item,
    dli.others_ration,
    dli.others_general,
    dli.others_hygiene,
    dli.cooked_food_no_of_meals,
    
    -- Dispatch status information
    ds.dispatch_id,
    ds.dispatch_name,
    ds.dispatch_date,
    ds.city,
    ds.state,
    ds.district,
    ds.country,
    -- having duplicate of the same columns for cross-filtering purposes
   
    ds.pincode,
    ds.street,
    ds.goonj_office,
    ds.transporter,
    ds.vehicle_number,
    ds.driver_name,
    ds.driver_contact_number,
    ds.e_waybill_number,
    ds.truck_vehicle_capacity,
    ds.total_no_of_bags_packages,
    ds.name_of_poc,
    ds.contact_no_of_poc,
    ds.poc_contact_details,
    ds.disaster_type,
    ds.from_which_processing_center as dispatch_from_processing_center,
    
    -- Timestamps
    dli.created_date as dispatch_line_item_created_date,
    ds.created_date as dispatch_created_date,
    k.kit_creation_date

FROM {{ ref('staging_kit') }} k
JOIN {{ ref('staging_dispatch_line_items') }} dli 
    ON k.kit_id = dli.kit_id
JOIN {{ ref('staging_dispatch_status') }} ds 
    ON dli.dispatch_status = ds.dispatch_id