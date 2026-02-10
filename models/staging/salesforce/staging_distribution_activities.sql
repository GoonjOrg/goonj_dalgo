-- Distribution activities data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['distribution_activities', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS distribution_activity_id,
    "Name" AS distribution_activity_name,
    "IsDeleted" AS is_deleted,
    "Activity__c" AS activity,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "LastViewedDate" AS last_viewed_date,
    "SystemModstamp" AS system_modstamp,
    "Distribution__c" AS distribution,
    "LastActivityDate" AS last_activity_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "LastReferencedDate" AS last_referenced_date,
    "Number_of_persons__c" AS number_of_persons,
    "Is_Created_from_Avni__c" AS is_created_from_avni,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id,
    "_airbyte_extracted_at" AS airbyte_extracted_at,
    "_airbyte_meta" AS airbyte_meta

FROM {{ source('staging_salesforce', 'distribution_activities') }}

WHERE
    -- Don't include deleted records
       "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False