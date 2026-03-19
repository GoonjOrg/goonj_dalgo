{{ config(
    materialized='table',
    tags=['dispatches_info', 'prod', 'salesforce']
) }}


WITH base AS (

select distinct
annual_year,
quarter,
monthnum,
month,
state,
district,
block,
processing_center_name,
processing_center_type,
sender_center_id,
processing_state,
processing_zone,
processing_district,
receiver_center_name,
--receiver_center_type,
receiver_state,
receiver_zone,
receiver_center_id,
receiver_district,
receiver_account_type,    
disaster_type,
dispatch_id,
dispatch_name,
dispatch_date,
demand_id,
demand_post_validation_id,
dpv_status,
dispatch_stage,
local_demand,
internal_demand,
remarks,
dispatch_line_item_id,
dispatch_line_item_name,
kit.kit_id,
kit.kit_name,
dispatches.quantity,
dispatches.unit,
dispatches.material_code,
dispatches.material_type,
dispatches.material_content,
dispatches.contributed_item,
dispatches.others,
dispatches.others_ration,
dispatches.others_general,
dispatches.type_of_material,
dispatches.material_inventory_name,
dispatches.item_category,
dispatches.item_sub_category,
dispatches.bulk_material,
dispatches.dump_material,
dispatches.othermaterial,
dispatches.dispatch_received_status_id,

kit.kit_type,
kit.kit_sub_type,
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
{{ ref('int_dispatches') }} as dispatches 
LEFT JOIN {{ ref('int_kit') }} as kit
    ON dispatches.kit_id = kit.kit_id

),

yearly_totals AS (
    SELECT 
        annual_year,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_year
    FROM base
    GROUP BY annual_year
),

quarterly_totals AS (
    SELECT 
        annual_year,
        quarter,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_quarter
    FROM base
    GROUP BY annual_year, quarter
),

yearly_state_totals AS (
    SELECT 
        annual_year,
        state,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_year_state
    FROM base
    GROUP BY annual_year, state
),

quarterly_state_totals AS (
    SELECT 
        annual_year,
        quarter,
        state,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_quarter_state
    FROM base
    GROUP BY annual_year, quarter, state
)

SELECT 
    b.*,
    yt.total_dispatches_year,
    qt.total_dispatches_quarter,
    yst.total_dispatches_year_state,
    qst.total_dispatches_quarter_state
FROM base b
LEFT JOIN yearly_totals yt ON b.annual_year = yt.annual_year
LEFT JOIN quarterly_totals qt ON b.annual_year = qt.annual_year AND b.quarter = qt.quarter
LEFT JOIN yearly_state_totals yst ON b.annual_year = yst.annual_year AND b.state = yst.state
LEFT JOIN quarterly_state_totals qst ON b.annual_year = qst.annual_year AND b.quarter = qst.quarter AND b.state = qst.state
