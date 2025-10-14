{{ config(
    materialized='table',
    tags=['distribution_analysis', 'prod', 'analytics']
) }}

-- Base CTE to calculate financial year and get required fields
WITH base_data AS (
    SELECT 
        distribution_id,
        state,
        district,
        block,
        other_block,
        village,
        type_of_initiative,
        no_of_families_reached,
        quantity,
        date_of_distribution,
        CASE 
            WHEN EXTRACT(MONTH FROM date_of_distribution) >= 4 
            THEN EXTRACT(YEAR FROM date_of_distribution)::text || '-' || RIGHT((EXTRACT(YEAR FROM date_of_distribution) + 1)::text, 2)
            ELSE (EXTRACT(YEAR FROM date_of_distribution) - 1)::text || '-' || RIGHT(EXTRACT(YEAR FROM date_of_distribution)::text, 2)
        END AS financial_year
    FROM {{ ref('int_distribution_summary') }}
    WHERE date_of_distribution IS NOT NULL
        AND date_of_distribution >= '2021-04-01'
),

-- Calculate state and district level metrics
state_district_metrics AS (
    SELECT 
        financial_year,
        state,
        district,
        COUNT(DISTINCT distribution_id) as distributions_count,
        --COUNT(DISTINCT CONCAT(
        --    state, '|', 
        --    district, '|', 
        --    COALESCE(block, ''), '|', 
        --    COALESCE(other_block, ''), '|', 
        --    COALESCE(village_name, 'Unknown')
        --)) as villages_reached,
        SUM(COALESCE(no_of_families_reached, 0) + COALESCE(quantity, 0)) as total_families_reached
    FROM base_data
    WHERE financial_year >= '2021-22'
    GROUP BY financial_year, state, district
)

-- Main query with all metrics
SELECT 
    financial_year,
    'All States' as states_reached,
    COUNT(DISTINCT state) as districts_reached,
    --SUM(villages_reached) as total_villages_reached,
    SUM(distributions_count) as total_distributions,
    NULL as disaster_types_covered,
    NULL as emergency_types_covered,
    SUM(total_families_reached) as total_families_reached,
    NULL as total_schools_reached,
    NULL as total_anganwadis_reached,
    NULL as total_educational_centers,
    CURRENT_TIMESTAMP as analysis_generated_at
FROM state_district_metrics
GROUP BY financial_year

UNION ALL

-- State level breakdown
SELECT 
    financial_year,
    state as states_reached,
    COUNT(DISTINCT district) as districts_reached,
    --SUM(villages_reached) as total_villages_reached,
    SUM(distributions_count) as total_distributions,
    NULL as disaster_types_covered,
    NULL as emergency_types_covered,
    SUM(total_families_reached) as total_families_reached,
    NULL as total_schools_reached,
    NULL as total_anganwadis_reached,
    NULL as total_educational_centers,
    CURRENT_TIMESTAMP as analysis_generated_at
FROM state_district_metrics
GROUP BY financial_year, state

UNION ALL

-- Initiative level breakdown
SELECT 
    bd.financial_year,
    bd.type_of_initiative as states_reached,
    NULL as districts_reached,
    --NULL as total_villages_reached,
    COUNT(DISTINCT bd.distribution_id) as total_distributions,
    NULL as disaster_types_covered,
    NULL as emergency_types_covered,
    SUM(COALESCE(bd.no_of_families_reached, 0) + COALESCE(bd.quantity, 0)) as total_families_reached,
    NULL as total_schools_reached,
    NULL as total_anganwadis_reached,
    NULL as total_educational_centers,
    CURRENT_TIMESTAMP as analysis_generated_at
FROM base_data bd
WHERE bd.financial_year IN ('2021-22', '2022-23', '2023-24', '2024-25', '2025-26', '2026-27')
AND bd.type_of_initiative IS NOT NULL
GROUP BY bd.financial_year, bd.type_of_initiative
