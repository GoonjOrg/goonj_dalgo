{{ config(
    materialized='table',
    tags=['dispatches_cy', 'prod', 'salesforce']
) }}


WITH base AS (

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
kit_id,
kit_name,
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
kit_sub_type,
type_of_material,
material_inventory_name,
item_category,
item_sub_category,
bulk_material,
dump_material,
othermaterial

FROM 
{{ ref('int_dispatches') }} as dispatches 

),



current_fy AS (
    SELECT 
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4 
            THEN EXTRACT(YEAR FROM CURRENT_DATE)::text || '-' || RIGHT((EXTRACT(YEAR FROM CURRENT_DATE) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM CURRENT_DATE) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM CURRENT_DATE)::text, 2)
        END AS current_financial_year
),


yearly_totals AS (
    SELECT 
        dispatch_year,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_year
    FROM base
    GROUP BY dispatch_year
),

quarterly_totals AS (
    SELECT 
        dispatch_year  ,
        dispatch_quarter,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_quarter
    FROM base
    GROUP BY dispatch_year, dispatch_quarter
),

yearly_state_totals AS (
    SELECT 
        dispatch_year,
        state,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_year_state
    FROM base
    GROUP BY dispatch_year, state
),

quarterly_state_totals AS (
    SELECT 
        dispatch_year,
        dispatch_quarter,
        state,
        COUNT(DISTINCT dispatch_id) AS total_dispatches_quarter_state
    FROM base
    GROUP BY dispatch_year, dispatch_quarter, state
)

SELECT 
    b.*,
    yt.total_dispatches_year,
    qt.total_dispatches_quarter,
    yst.total_dispatches_year_state,
    qst.total_dispatches_quarter_state
FROM base b
LEFT JOIN yearly_totals yt ON b.dispatch_year = yt.dispatch_year
LEFT JOIN quarterly_totals qt ON b.dispatch_year = qt.dispatch_year AND b.dispatch_quarter = qt.dispatch_quarter
LEFT JOIN yearly_state_totals yst ON b.dispatch_year = yst.dispatch_year AND b.state = yst.state
LEFT JOIN quarterly_state_totals qst ON b.dispatch_year = qst.dispatch_year AND b.dispatch_quarter = qst.dispatch_quarter AND b.state = qst.state
CROSS JOIN current_fy cfy
where b.dispatch_year=cfy.current_financial_year