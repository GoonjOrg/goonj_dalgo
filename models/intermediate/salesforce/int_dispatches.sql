{{ config(
    materialized='table',
    tags=['dispatches', 'intermediate', 'salesforce']
) }}

WITH base_dates AS (
    SELECT 
        *,
        EXTRACT(MONTH FROM dispatch_date) as m_num,
        EXTRACT(YEAR FROM dispatch_date) as y_num,
        TO_CHAR(dispatch_date, 'Mon') as month_name
    FROM {{ ref('staging_dispatch_status') }}
),
dispatches_calculated_dates AS (
    SELECT 
        *,
        CASE 
            WHEN m_num >= 4 THEN y_num::text || '-' || RIGHT((y_num + 1)::text, 2)
            ELSE (y_num - 1)::text || '-' || RIGHT(y_num::text, 2)
        END AS annual_year,
        CASE 
            WHEN m_num BETWEEN 4 AND 6 THEN 'Q1'
            WHEN m_num BETWEEN 7 AND 9 THEN 'Q2'
            WHEN m_num BETWEEN 10 AND 12 THEN 'Q3'
            WHEN m_num BETWEEN 1 AND 3 THEN 'Q4'
        END AS quarter
    FROM base_dates
)

SELECT DISTINCT
    -- Dispatch Info
    df.dispatch_id,
    df.dispatch_name,   
    df.dispatch_date,
    df.city,
    df.state,
    df.district,
    df.block,
    df.country,
    df.pincode,
    df.street,
    df.address_id,
    df.goonj_office,
    df.transporter,
    df.transporter_consignment_no,
    df.vehicle_number,
    df.truck_vehicle_capacity,
    df.e_waybill_number,
    df.total_no_of_bags_packages,
    df.loading_and_truck_images_link,
    df.name_of_poc, 
    df.demand_id,
    d.demand_name,
    df.demand_post_validation_id,
    d.internal_demand,
    d.local_demand,
    df.receiving_account_id,
    df.rate,
    df.remarks,
    
    -- Date Info
    df.annual_year,
    df.quarter,
    df.month_name as month,
    df.m_num as monthnum,

    d.disaster_type,
    d.post_validation_status as dpv_status,
    d.dispatch_stage as dispatch_stage,

    -- Account Info
    sender.account_name as processing_center_name,
    sender.account_type as processing_center_type,
    sender.state as processing_state,
    sender.district as procssing_district,
    receiver.account_name as receiver_center_name,
    receiver.account_type as receiver_center_type,
    receiver.state as receiver_state,
    receiver.district as receiver_district,
    CASE WHEN receiver.account_name LIKE '%Goonj%' THEN 'Self' ELSE 'Partner' END AS receiver_account_type,
    
     -- Dispatch line item information
    dli.dispatch_line_item_id,
    dli.dispatch_line_item_name,
    dli.kit_id,
    dli.quantity,
    dli.unit,
    dli.material_code,
    dli.material_type,
    dli.material_content,
    dli.contributed_item,
    dli.others,
    dli.others_ration,
    dli.others_general,

    --Material Info
    mi.type_of_material,
    mi.material_inventory_name,
    mi.item_category,
    mi.item_sub_category,
    mi.bulk_material,
    mi.dump_material,
    mi.other as othermaterial,

    -- Kit Info
    kit.kit_name,
    kit.kit_type,
    kit.kit_sub_type,

    
    drs.material_received_status_id as dispatch_received_status_id,
    drs.material_received_status_name as dispatch_received_status_name,
    drsli.dispatch_received_status_line_item_id as drsli_id,
    drsli.dispatch_received_status_line_item_name as drsli_name,
    drsli.item_name

FROM dispatches_calculated_dates df
LEFT JOIN {{ ref('int_demands') }} d ON df.demand_id = d.demand_id
LEFT JOIN {{ ref('staging_account') }} sender ON d.assigned_processing_center = sender.account_id
LEFT JOIN {{ ref('staging_account') }} receiver ON df.receiving_account_id = receiver.account_id
LEFT JOIN {{ ref('staging_dispatch_line_items') }} dli ON df.dispatch_id = dli.dispatch_status
LEFT JOIN {{ ref('staging_kit') }} kit ON dli.kit_id = kit.kit_id
LEFT JOIN {{ ref('staging_material_inventory') }} mi ON dli.material_inventory_id = mi.material_inventory_id
LEFT JOIN {{ ref('staging_material_received_status') }} drs ON df.dispatch_id = drs.dispatch_id
LEFT JOIN {{ ref('staging_dispatch_received_status_line_item') }} drsli 
    ON drsli.dispatch_received_status_id = drs.material_received_status_id
    AND drsli.item_name IN (kit.kit_name, dli.contributed_item, mi.material_inventory_name)