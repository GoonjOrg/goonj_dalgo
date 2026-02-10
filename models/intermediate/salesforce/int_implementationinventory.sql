{{ config(
    materialized='table',
    tags=['implementation_inventory', 'intermediate', 'salesforce']
) }}

WITH base_inventory AS (
    
    SELECT 
        *,
        EXTRACT(MONTH FROM date_of_receiving) AS m_num,
        EXTRACT(YEAR FROM date_of_receiving) AS y_num
    FROM {{ ref('staging_implementation_inventory') }}
),

implementation_inventory_calculated_fields AS (
    SELECT
        *,
        TO_CHAR(date_of_receiving, 'Mon') as receiving_month_name,
        CASE 
            WHEN EXTRACT(MONTH FROM date_of_receiving) >= 4 
            -- Logic: Year - (Year + 1)
            THEN TO_CHAR(date_of_receiving, 'YYYY') || '-' || TO_CHAR(date_of_receiving + interval '1 year', 'YY')
            ELSE TO_CHAR(date_of_receiving - interval '1 year', 'YYYY') || '-' || TO_CHAR(date_of_receiving, 'YY')
        END as receiving_financial_year,
        CASE 
            WHEN m_num BETWEEN 4 AND 6 THEN 'Q1'
            WHEN m_num BETWEEN 7 AND 9 THEN 'Q2'
            WHEN m_num BETWEEN 10 AND 12 THEN 'Q3'
            ELSE 'Q4' 
        END AS receiving_quarter
    FROM base_inventory
)

SELECT
    -- Basic system information
    implementation_inventory_id,
    implementation_inventory_name,
    unit,
    remarks,
    created_by_id,
    created_date,
    last_viewed_date,
    last_activity_date,
    last_modified_by_id,
    last_modified_date,
    last_referenced_date,
    -- Material and kit information
    material_name,
    material_type,
    original_name,
    kit_type,
    sub_type,
    material_sub_category,
    material_kit_id,
    material_kit_name,
    purchase_kit_name,
    other_material_name,
    -- Quantity and tracking
    current_quantity,
    original_quantity,
    date_of_receiving,
    -- Location and center information
    center_field_office,
    center_field_office_state,
    center_field_office_district,
    -- Source and account information
    from_which_account,
    source_of_material,
    created_from,
    created_or_received,
    purchased_created_received,
    -- Dispatch information
    dispatch_id,
    dispatch_line_item_id,
    dispatch_received_status,
    dispatch_received_status_line_item,
    -- Additional fields
    bill_name,
    unique_id,
    vehicle_type,
    -- Optimized Date fields
    m_num AS receiving_month,
    y_num AS receiving_year,
    receiving_month_name,
    receiving_financial_year,
    receiving_quarter
FROM implementation_inventory_calculated_fields