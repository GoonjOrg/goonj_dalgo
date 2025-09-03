{{ config(
    materialized='table',
    tags=['kit_dispatch', 'prod', 'salesforce']
) }}

SELECT
*
FROM 
{{ ref('int_kit_dispatch') }}