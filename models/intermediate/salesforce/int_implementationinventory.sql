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
        END as receiving_year,
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
    ii.unit,
    ii.remarks,
    ii.created_by_id,
    ii.created_date,
    ii.last_viewed_date,
    ii.last_activity_date,
    ii.last_modified_by_id,
    ii.last_modified_date,
    ii.last_referenced_date,
    -- Material and kit information
    ii.material_name,
    ii.material_type,
    ii.original_name,
    ii.kit_type,
    ii.sub_type,
    ii.material_sub_category,
    ii.material_kit_id as kit_id,
    kit.kit_name,
    ii.purchase_kit_id,
    purchasedkit.kit_name as purchase_kit_name,
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
    accounts.account_name,
    accounts.account_type,
    ii.from_which_account,
    ii.source_of_material,
    ii.created_from,
    ii.created_or_received,
    ii.purchased_created_received,
    -- Dispatch information
    ii.dispatch_id,
    ii.dispatch_line_item_id,
    ii.dispatch_received_status,
    ii.dispatch_received_status_line_item,
    -- Additional fields
    ii.bill_name,
    ii.unique_id,
    ii.vehicle_type,
    -- Optimized Date fields
    ii.m_num AS receiving_monthnum,
    ii.receiving_month_name,
    ii.receiving_year,
    ii.receiving_quarter
FROM implementation_inventory_calculated_fields ii
left join {{ ref('staging_account')}} accounts
on ii.center_field_office = accounts.account_id
left join {{ ref('staging_kit')}} kit
on ii.material_kit_id = kit.kit_id
left join {{ ref('staging_kit')}} purchasedkit
on ii.purchase_kit_id = purchasedkit.kit_id