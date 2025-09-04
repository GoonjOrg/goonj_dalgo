SELECT *
FROM {{ source('staging_salesforce', 'distribution') }}
LIMIT 1
