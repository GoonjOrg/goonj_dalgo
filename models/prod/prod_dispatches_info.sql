{{ config(
    materialized='table',
    tags=['dispatches_info', 'prod', 'salesforce']
) }}


select
annual_year,
quarter,
monthnum,
month,
state,
district,
block,
processing_center_name,
processing_center_type,
processing_state,
procssing_district,
receiver_center_name,
receiver_center_type,
receiver_state,
receiver_district,
dispatched_account_type    
disaster_type,
dispatch_name,
dispatch_date,
demand_id,
demand_post_validation_id,
local_demand,
internal_demand,
remarks,
dispatch_line_item_id,
dispatch_line_item_name,
kit_id,
quantity,
unit,
material_code,
material_type,
material_content,
contributed_item,
others,
others_ration,
others_general,
kit_type,
kit_sub_type

FROM 
{{ ref('int_dispatches') }} as dispatches 


