{{ config(
    materialized='table',
    tags=['distribution_geography_summary', 'prod', 'salesforce']
) }}



select *
    from {{ ref('int_distribution_geography') }} 
