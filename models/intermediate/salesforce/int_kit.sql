{{ config(
    materialized='table',
    tags=['kit', 'intermediate', 'salesforce']
) }}

SELECT 
    -- Kit information
    k.kit_id,
    k.kit_name,
    k.kit_type,
    k.kit_sub_type,
    k.kit_source,
    k.kit_status,
    k.quantity as kit_quantity,
    k.current_quantity,
    k.original_quantity,
    k.processing_center,
    k.depreciated,
    k.other_kit_detail,
    k.kit_creation_date,
    k.number_of_people_involved,
    k.remarks as kit_remarks,
    k.created_date as kit_created_date,
    k.last_modified_date as kit_last_modified_date,
    
    -- Kit line item information
    kli.kit_line_item_id,
    kli.kit_line_item_name,
    kli.quantity as line_item_quantity,
    kli.unit,
    kli.contributed_item,
    kli.created_date as line_item_created_date,
    kli.last_modified_date as line_item_last_modified_date,
    
    -- Material inventory information (when available)
    mi.material_inventory_id,
    mi.material_inventory_name,
    mi.item_name,
    mi.item_category,
    mi.item_sub_category,
    mi.type_of_material,
    mi.unit_of_measurement,
    mi.quantity as material_quantity,
    mi.processing_center as material_processing_center,
    mi.active as material_active,
    mi.remarks as material_remarks,

    ii.implementation_inventory_id,
    ii.implementation_inventory_name,
   
    
    CASE 
        WHEN kli.contributed_item IS NOT NULL THEN 'Contributed Item'
        WHEN mi.material_inventory_id IS NOT NULL THEN 'Material Inventory'
        ELSE 'Kit Only'
    END as item_type,

    CASE 
        WHEN kli.implementation_inventory_id IS NOT NULL THEN 'Implemention'
        WHEN kli.material_inventory_id IS NOT NULL THEN 'Processing'
    END as center_type
    
    

FROM {{ ref('staging_kit') }} k
LEFT JOIN {{ ref('staging_kit_line_item') }} kli 
    ON k.kit_id = kli.kit
LEFT JOIN {{ ref('staging_material_inventory') }} mi 
    ON kli.material_inventory_id = mi.material_inventory_id
LEFT JOIN {{ ref('staging_implementation_inventory') }} ii 
    ON kli.implementation_inventory_id = ii.implementation_inventory_id