{{ config(
    materialized='table',
    tags=['dispatches', 'intermediate', 'salesforce']
) }}

SELECT 
    -- Dispatch status information
    ds.dispatch_id,
    ds.dispatch_name,   
    ds.dispatch_date,
    ds.city,
    ds.state,
    ds.district,
    ds.block,
    ds.country,
    ds.pincode,
    ds.street,
    ds.address_id,
    ds.goonj_office,
    ds.from_which_processing_center,
    ds.transporter,
    ds.transporter_consignment_no,
    ds.vehicle_number,
    ds.truck_vehicle_capacity,
    ds.e_waybill_number,
    ds.total_no_of_bags_packages,
    ds.loading_and_truck_images_link,
    ds.name_of_poc,
    ds.demand_id,
    ds.demand_post_validation_id,
    ds.local_demand,
    ds.internal_demand,
    ds.receiving_account_id,
    ds.rate,
    ds.remarks,
    d.disaster_type,
    
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
    
    dpv.status as status,
    dpv.dispatch_stage as dispatch_stage,

    kit.kit_type,
    kit.kit_sub_type,
    
    CASE 
        WHEN EXTRACT(MONTH FROM ds.dispatch_date) >= 4 
            THEN EXTRACT(YEAR FROM ds.dispatch_date)::text || '-' || RIGHT((EXTRACT(YEAR FROM ds.dispatch_date) + 1)::text, 2)
        ELSE (EXTRACT(YEAR FROM ds.dispatch_date) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM ds.dispatch_date)::text, 2)
    END AS annual_year,
    CASE 
        WHEN EXTRACT(MONTH FROM ds.dispatch_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ds.dispatch_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ds.dispatch_date) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM ds.dispatch_date) BETWEEN 1 AND 3 THEN 'Q4'
    END AS quarter,
    TO_CHAR(ds.dispatch_date, 'Mon') as month,
    EXTRACT(MONTH FROM ds.dispatch_date) as monthnum,

    senderaccount.account_name as processing_center_name,
    senderaccount.account_type as processing_center_type,
    senderaccount.state as processing_state,
    senderaccount.district as procssing_district,
    receiveraccount.account_name as receiver_center_name,
    receiveraccount.account_type as receiver_center_type,
    receiveraccount.state as receiver_state,
    receiveraccount.district as receiver_district,
    case when receiveraccount.account_name like '%Goonj%' then 'Self' else 'Partner' end as dispatched_account_type    

FROM {{ ref('staging_dispatch_status') }} ds 
left join 
{{ref('staging_dispatch_line_items')}} dli on ds.dispatch_id = dli.dispatch_status
left join
{{ref('staging_account')}} senderaccount on ds.from_which_processing_center = senderaccount.account_id
left join
{{ref('staging_account')}} receiveraccount on ds.receiving_account_id = receiveraccount.account_id
left join
{{ref('staging_kit')}} kit on dli.kit_id = kit.kit_id
left join
{{ref('staging_relief_requirement')}} d on ds.demand_id = d.demand_id
left join
{{ref('staging_demand_post_validation')}} dpv on ds.demand_post_validation_id = dpv.demand_post_validation_id
where ds.is_deleted=False and dli.is_deleted=False
