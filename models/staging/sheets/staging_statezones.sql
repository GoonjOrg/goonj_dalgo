{{ config(
    materialized='table',
    tags=['statezones', 'staging', 'sheets', 'data_extraction']
) }}

SELECT
zone,
state
FROM {{ source('staging_sheets', 'state_zones') }}
