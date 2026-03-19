{{ config(
    materialized='table',
    tags=['implementation_inventory', 'prod', 'salesforce']
) }}

select 
 -- Basic system information
    implementation_inventory_id,
    implementation_inventory_name,
    ii.unit,
    ii.remarks,
    -- Material and kit information
    ii.material_name,
    ii.material_type,
    ii.original_name,
    ii.kit_type,
    ii.sub_type,
    ii.material_sub_category,
    ii.kit_id,
    ii.kit_name,
    ii.purchase_kit_id,
    ii.purchase_kit_name,
    ii.other_material_name,
    -- Quantity and tracking
    ii.current_quantity,
    ii.original_quantity,
    ii.date_of_receiving,
    -- Location and center information
    ii.center_field_office,
    ii.center_field_office_state,
    ii.center_field_office_district,
    -- Source and account information
    ii.account_name,
    ii.account_type,
    ii.from_which_account,
    ii.source_of_material,
    ii.created_from,
    ii.created_or_received,
    ii.purchased_created_received,
    ii.receiving_monthnum,
    ii.receiving_month_name,
    ii.receiving_year,
    ii.receiving_quarter,
    -- Dispatch information
    ii.dispatch_id,
    ii.dispatch_line_item_id,
    dispatches.dispatch_name,
    dispatches.dispatch_line_item_name,
    dispatches.dispatch_date,
    dispatches.dpv_status,
    dispatches.dispatch_stage
    
 from 
{{ ref('int_implementationinventory') }}ii
left join {{ref('int_dispatches')}} dispatches
on dispatches.dispatch_id = ii.dispatch_id and ii.dispatch_line_item_id = dispatches.dispatch_line_item_id