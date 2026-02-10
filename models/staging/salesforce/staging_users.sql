-- Account data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['users', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS id,
    "Name" AS name,
    "Email" AS email,
    "Username" AS username,
    "IsActive" AS is_active
FROM {{ source('staging_salesforce', 'users') }}
