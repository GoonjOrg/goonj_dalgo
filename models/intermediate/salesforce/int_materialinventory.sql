{{ config(
    materialized='table',
    tags=['material_inventory', 'intermediate', 'salesforce']
) }}


SELECT
    -- Basic system information
    mi.material_inventory_id,
    mi.material_inventory_name,
    mi.other,
    mi.store,
    mi.active,
    mi.company,
    mi.remarks,
    mi.created_by_id,
    mi.created_date,
    mi.last_modified_by_id,
    mi.last_modified_date,
    mi.record_type_id,
    -- Material information
    mi.item_name,
    mi.item_category,
    mi.item_sub_category,
    mi.type_of_material,
    mi.raw_material_category,
    mi.purchased_item_name,
    mi.sub_area,
    -- Quantity and measurement
    mi.quantity,
    mi.unit_of_measurement,
    mi.raw_unit_of_measurement,
    mi.maximum_threshold,
    mi.minimum_threshold,
    mi.sum_of_material_quantity_history,
    -- Location and processing
    mi.processing_center,
    -- Material classification
    mi.bulk_material,
    mi.dump_material,
    mi.vehicle_category,
    mi.color_code_for_raw_material,
    CASE 
        WHEN quantity <= minimum_threshold THEN 'Below Minimum'
        WHEN quantity >= maximum_threshold THEN 'Above Maximum'
        WHEN quantity BETWEEN minimum_threshold AND maximum_threshold THEN 'Within Range'
        ELSE 'Unknown'
    END as threshold_status

FROM {{ ref('staging_material_inventory') }} mi 
LEFT JOIN {{ref('staging_account')}} center
    ON mi.processing_center = center.account_id
    WHERE 
    center.account_type !='Production Center'
