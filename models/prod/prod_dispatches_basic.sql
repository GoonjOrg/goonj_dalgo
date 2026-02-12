{{ config(
    materialized='table',
    tags=['dispatches_basic', 'prod', 'salesforce']
) }}

WITH base AS (
    SELECT  
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
    remarks
FROM 
{{ ref('int_dispatches_basic') }} as dispatches 
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


