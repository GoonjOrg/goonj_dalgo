{{ config(
    materialized='table',
    tags=['production_inventory', 'intermediate', 'salesforce']
) }}
SELECT
    -- Basic system information
    mi.material_inventory_id,
    mi.material_inventory_name,
    mi.other,
    mi.active,
    mi.remarks,
    mi.created_by_id,
    mi.created_date,
    mi.quantity,
    mi.sub_area,
    mi.item_name,
    mi.record_type_id,
    mi.last_viewed_date,
    mi.system_modstamp,
    mi.company_name,
    mi.bulk_material,
    mi.dump_material,
    mi.item_category,
    mi.last_activity_date,
    mi.last_modified_by_id,
    mi.last_modified_date,
    mi.last_referenced_date,
    -- Material inventory specific fields
    mi.type_of_material,
    mi.vehicle_category,
    mi.item_sub_category,
    mi.maximum_threshold,
    mi.minimum_threshold,
    mi.processing_center as production_center,
    mi.purchased_item_name,
    mi.unit_of_measurement,
    mi.raw_material_category,
    mi.raw_unit_of_measurement,
    mi.color_code_for_raw_material,
    mi.sum_of_material_quantity_history,
    
    CASE 
        WHEN bulk_material = TRUE THEN 'Bulk Material'
        WHEN dump_material = TRUE THEN 'Dump Material'
        ELSE 'Regular Material'
    END as production_material_classification,
    
    CASE 
        WHEN active = TRUE THEN 'Active'
        WHEN active = FALSE THEN 'Inactive'
        ELSE 'Unknown'
    END as production_status,
    
    CASE 
        WHEN quantity <= minimum_threshold THEN 'Below Minimum'
        WHEN quantity >= maximum_threshold THEN 'Above Maximum'
        ELSE 'Within Range'
    END as inventory_threshold_status
    
    
FROM {{ ref('staging_material_inventory') }} mi
LEFT JOIN {{ref('staging_account')}} center
ON mi.processing_center = center.account_id
WHERE 
center.account_type='Production Center'