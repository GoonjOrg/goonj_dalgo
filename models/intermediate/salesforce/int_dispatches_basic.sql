{{ config(
    materialized='table',
    tags=['dispatches_basic', 'intermediate', 'salesforce']
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
    dcd.dispatch_id,
    dcd.dispatch_name,   
    dcd.dispatch_date,
    dcd.city,
    dcd.state,
    dcd.district,
    dcd.block,
    dcd.country,
    dcd.pincode,
    dcd.street,
    dcd.address_id,
    dcd.goonj_office,
    dcd.from_which_processing_center,
    dcd.transporter,
    dcd.transporter_consignment_no,
    dcd.vehicle_number,
    dcd.truck_vehicle_capacity,
    dcd.e_waybill_number,
    dcd.total_no_of_bags_packages,
    dcd.loading_and_truck_images_link,
    dcd.name_of_poc,
    dcd.demand_id,
    d.demand_name,
    dcd.demand_post_validation_id,
    d.internal_demand,
    d.local_demand,
    dcd.receiving_account_id,
    dcd.rate,
    dcd.remarks,
    
    -- Date Info
    dcd.annual_year,
    dcd.quarter,
    dcd.month_name as month,
    dcd.m_num as monthnum,

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
    CASE WHEN receiver.account_name LIKE '%Goonj%' THEN 'Self' ELSE 'Partner' END AS receiver_account_type

FROM dispatches_calculated_dates dcd
LEFT JOIN {{ ref('int_demands') }} d ON dcd.demand_id = d.demand_id
LEFT JOIN {{ ref('staging_account') }} sender ON dcd.from_which_processing_center = sender.account_name
LEFT JOIN {{ ref('staging_account') }} receiver ON dcd.receiving_account_id = receiver.account_id
