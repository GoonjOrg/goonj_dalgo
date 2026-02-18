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
procssing_district,
receiver_center_name,
--receiver_center_type,
receiver_state,
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

case when kit.line_item_quantity is null then dispatches.quantity else kit.line_item_quantity*dispatches.quantity end as item_quantity    

FROM 
{{ ref('int_dispatches') }} as dispatches 
left join {{ ref('int_kit')}} as kit
    on dispatches.kit_id = kit.kit_id
cross join current_fy cfy
where kit.contributed_item is null
and kit.type_of_material !='Contributed_Track'
and kit.item_category !='Admin Material'
and dispatches.annual_year=cfy.current_financial_year