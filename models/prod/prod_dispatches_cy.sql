{{ config(
    materialized='table',
    tags=['dispatches_cy', 'prod', 'salesforce']
) }}

WITH 

current_fy AS (
    SELECT 
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4 
            THEN EXTRACT(YEAR FROM CURRENT_DATE)::text || '-' || RIGHT((EXTRACT(YEAR FROM CURRENT_DATE) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM CURRENT_DATE) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM CURRENT_DATE)::text, 2)
        END AS current_financial_year
),

-- Pre-filter dispatches for current financial year
filtered_dispatches AS (
    SELECT *
    FROM {{ ref('int_dispatches') }}
    WHERE annual_year = (SELECT current_financial_year FROM current_fy)
    AND ((item_category IS NULL) OR (item_category != 'Admin Material'))
    AND contributed_item IS NULL
    --AND type_of_material != 'Contributed_Track'
    
),

-- Pre-filter kits to avoid scanning entire kit table
filtered_kits AS (
    SELECT *
    FROM {{ ref('int_kit') }}
    WHERE contributed_item IS NULL
        AND type_of_material != 'Contributed_Track'
        AND item_category != 'Admin Material'
)

select distinct
annual_year as dispatch_year,
quarter as dispatch_quarter,
monthnum as dispatch_monthnum,
month as dispatch_month,
state,
district,
block,
processing_center_name,
processing_center_type,
processing_state,
processing_zone,
processing_district,
receiver_center_name,
--receiver_center_type,
receiver_state,
receiver_zone,
receiver_district,
receiver_account_type,    
disaster_type,
dispatch_id,
dispatch_name,
dispatch_date,
demand_id,
demand_name,
demand_post_validation_id,
dpv_status,
dispatch_stage,
local_demand,
internal_demand,
remarks,
dispatch_line_item_id,
dispatch_line_item_name,
dispatches.kit_id,
dispatches.kit_name,
dispatches.quantity,
dispatches.unit,
dispatches.material_code,
dispatches.material_type,
dispatches.material_content,
dispatches.contributed_item,
dispatches.others,
dispatches.others_ration,
dispatches.others_general,
dispatches.kit_type,
dispatches.kit_sub_type,
dispatches.type_of_material,
dispatches.material_inventory_name,
dispatches.item_category,
dispatches.item_sub_category,
dispatches.bulk_material,
dispatches.dump_material,
dispatches.othermaterial,
dispatches.truck_vehicle_capacity,
dispatches.total_no_of_bags_packages,
dispatches.transporter_consignment_no,
dispatches.transporter,

kit.kit_line_item_name,
kit.line_item_quantity as kit_line_quantity,
kit.material_inventory_name as kit_line_material_inventory_name,
kit.item_name as kit_line_items_name,
kit.item_category as kit_line_item_category,
kit.item_sub_category as kit_line_item_sub_category,
kit.type_of_material as kit_line_type_of_material,

COALESCE(
    CASE 
        WHEN kit.kit_id IS NOT NULL AND kit.line_item_quantity IS NOT NULL 
        THEN kit.line_item_quantity * dispatches.quantity
        ELSE dispatches.quantity
    END, 
    0
) as item_quantity,


CASE when kit.kit_id is not null then kit.item_category 
else dispatches.item_category END as material_category,

CASE when kit.kit_id is not null then kit.item_sub_category 
else dispatches.item_sub_category END as material_sub_category



FROM 
filtered_dispatches as dispatches 
LEFT JOIN filtered_kits as kit
    ON dispatches.kit_id = kit.kit_id