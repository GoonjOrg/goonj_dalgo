-- Dispatch address data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['dispatch_address', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS dispatch_address_id,
    "Name" AS dispatch_address_name,
    "City__c" AS city,
    "OwnerId" AS owner_id,
    "Block__c" AS block,
    "Other__c" AS other,
    "State__c" AS state,
    "IsDeleted" AS is_deleted,
    "Street__c" AS street,
    "Account__c" AS account,
    "Country__c" AS country,
    "Village__c" AS village,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "District__c" AS district,
    "LastViewedDate" AS last_viewed_date,
    "Other_Block__c" AS other_block,
    "Postal_Code__c" AS postal_code,
    "SystemModstamp" AS system_modstamp,
    "Tola_Mohalla__c" AS tola_mohalla,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "Selected_State__c" AS selected_state,
    "LastReferencedDate" AS last_referenced_date,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'dispatch_address') }}

WHERE
    -- Don't include deleted records
    "IsDeleted" = FALSE
    
    -- Make sure we have the basic information we need
    AND "Id" IS NOT NULL
    AND "CreatedDate" IS NOT NULL
    
    -- Don't include completely empty or invalid records
    AND "Name" IS NOT NULL
    AND "Name" != ''

ORDER BY "CreatedDate" DESC, "Id"
