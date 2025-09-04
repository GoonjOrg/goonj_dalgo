{{ config(
    materialized='table',
    tags=['distribution_analysis', 'prod', 'analytics']
) }}

WITH financial_year_data AS (
    SELECT *,
        CASE 
            WHEN date_of_distribution >= '2025-04-01' AND date_of_distribution <= '2026-03-31' 
            THEN 'FY2025-26'
            ELSE 'Other'
        END AS financial_year
    FROM {{ ref('int_distribution_summary') }}
    WHERE date_of_distribution IS NOT NULL
),

state_district_reach AS (
    SELECT 
        financial_year,
        state,
        district,
        COUNT(DISTINCT distribution_id) as distributions_count,
        COUNT(DISTINCT CONCAT(state, '|', district, '|', COALESCE(block, ''), '|', COALESCE(other_block, ''), '|', locality_village_name)) as villages_reached
    FROM financial_year_data
    WHERE financial_year = 'FY2025-26'
    AND locality_village_name IS NOT NULL
    GROUP BY financial_year, state, district
),

-- disaster_analysis AS (
--     SELECT 
--         financial_year,
--         disaster_type,
--         emergency_type,
--         COUNT(DISTINCT distribution_id) as distributions_count,
--         COUNT(DISTINCT state) as states_reached,
--         COUNT(DISTINCT district) as districts_reached
--     FROM financial_year_data
--     WHERE financial_year = 'FY2025-26'
--     GROUP BY financial_year, disaster_type, emergency_type
-- ),

families_reached_analysis AS (
    SELECT 
        financial_year,
        state,
        district,
        type_of_initiative,
        -- disaster_type,
        SUM(COALESCE(no_of_families_reached, 0) + COALESCE(quantity, 0)) as total_families_reached,
        COUNT(DISTINCT distribution_id) as distributions_count
    FROM financial_year_data
    WHERE financial_year = 'FY2025-26'
    GROUP BY financial_year, state, district, type_of_initiative -- , disaster_type
),

-- educational_institutions AS (
--     SELECT 
--         financial_year,
--         state,
--         district,
--         type_of_educational_entity,
--         COUNT(DISTINCT CASE 
--             WHEN LOWER(type_of_educational_entity) LIKE '%school%' 
--             OR LOWER(school_aanganwadi_learning_center_name) LIKE '%school%'
--             THEN distribution_id 
--         END) as schools_count,
--         COUNT(DISTINCT CASE 
--             WHEN LOWER(type_of_educational_entity) LIKE '%anganwadi%' 
--             OR LOWER(school_aanganwadi_learning_center_name) LIKE '%anganwadi%'
--             THEN distribution_id 
--         END) as anganwadis_count,
--         COUNT(DISTINCT school_aanganwadi_learning_center_name) as total_educational_centers
--     FROM financial_year_data
--     WHERE financial_year = 'FY2025-26'
--     AND (type_of_educational_entity IS NOT NULL OR school_aanganwadi_learning_center_name IS NOT NULL)
--     GROUP BY financial_year, state, district, type_of_educational_entity
-- ),

final_analysis AS (
SELECT 
    -- Financial Year
    'FY2025-26' as financial_year,
    
    -- Geographic Reach
    COUNT(DISTINCT sdr.state)::text as states_reached,
    COUNT(DISTINCT sdr.district) as districts_reached,
    SUM(sdr.villages_reached) as total_villages_reached,
    
    -- Distribution Summary
    SUM(sdr.distributions_count) as total_distributions,
    
    -- Disaster Analysis (fields not available)
    NULL as disaster_types_covered,
    NULL as emergency_types_covered,
    
    -- Families Reached Analysis
    SUM(fra.total_families_reached) as total_families_reached,
    
    -- Educational Institutions (fields not available)
    NULL as total_schools_reached,
    NULL as total_anganwadis_reached,
    NULL as total_educational_centers,
    
    -- Current timestamp
    CURRENT_TIMESTAMP as analysis_generated_at

FROM state_district_reach sdr
-- LEFT JOIN disaster_analysis da ON sdr.financial_year = da.financial_year
LEFT JOIN families_reached_analysis fra ON sdr.financial_year = fra.financial_year 
    AND sdr.state = fra.state 
    AND sdr.district = fra.district
-- LEFT JOIN educational_institutions ei ON sdr.financial_year = ei.financial_year 
--     AND sdr.state = ei.state 
--     AND sdr.district = ei.district
WHERE sdr.financial_year = 'FY2025-26'

UNION ALL

-- State-wise breakdown
SELECT 
    fra.financial_year,
    fra.state as states_reached,
    COUNT(DISTINCT fra.district) as districts_reached,
    COUNT(DISTINCT CONCAT(sdr.state, '|', sdr.district, '|', COALESCE(sdr.block, ''), '|', COALESCE(sdr.other_block, ''), '|', sdr.locality_village_name)) as total_villages_reached,
    COUNT(DISTINCT sdr.distribution_id) as total_distributions,
    NULL as disaster_types_covered,
    NULL as emergency_types_covered,
    SUM(fra.total_families_reached) as total_families_reached,
    NULL as total_schools_reached,
    NULL as total_anganwadis_reached,
    NULL as total_educational_centers,
    CURRENT_TIMESTAMP as analysis_generated_at

FROM families_reached_analysis fra
LEFT JOIN financial_year_data sdr ON fra.financial_year = sdr.financial_year 
    AND fra.state = sdr.state 
    AND fra.district = sdr.district
-- LEFT JOIN educational_institutions ei ON fra.financial_year = ei.financial_year 
--     AND fra.state = ei.state 
--     AND fra.district = ei.district
WHERE fra.financial_year = 'FY2025-26'
AND sdr.locality_village_name IS NOT NULL
GROUP BY fra.financial_year, fra.state

UNION ALL

-- Initiative-wise breakdown
SELECT 
    fra.financial_year,
    fra.type_of_initiative as states_reached,
    NULL as districts_reached,
    NULL as total_villages_reached,
    COUNT(DISTINCT sdr.distribution_id) as total_distributions,
    NULL as disaster_types_covered,
    NULL as emergency_types_covered,
    SUM(fra.total_families_reached) as total_families_reached,
    NULL as total_schools_reached,
    NULL as total_anganwadis_reached,
    NULL as total_educational_centers,
    CURRENT_TIMESTAMP as analysis_generated_at

FROM families_reached_analysis fra
LEFT JOIN financial_year_data sdr ON fra.financial_year = sdr.financial_year 
    AND fra.state = sdr.state 
    AND fra.district = sdr.district
WHERE fra.financial_year = 'FY2025-26'
AND fra.type_of_initiative IS NOT NULL
GROUP BY fra.financial_year, fra.type_of_initiative
)

SELECT * FROM final_analysis
