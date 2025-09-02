-- Account data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['accounts', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS account_id,
    "Name" AS account_name,
    "OwnerId" AS owner_id,
    "CreatedById" AS created_by_id,
    "CreatedDate" AS created_date,
    "LastModifiedById" AS last_modified_by_id,
    "LastModifiedDate" AS last_modified_date,
    "LastViewedDate" AS last_viewed_date,
    "LastReferencedDate" AS last_referenced_date,
    "SystemModstamp" AS system_modstamp,
    "IsDeleted" AS is_deleted,

    -- Account classification
    "Type" AS account_type,
    "Industry" AS industry,
    "AccountSource" AS account_source,
    "ParentId" AS parent_id,
    "RecordTypeId" AS record_type_id,

    -- Contact information
    "Phone" AS phone,
    "Fax" AS fax,
    "Website" AS website,
    "Email_ID__c" AS email,

    -- Address information
    "BillingStreet" AS billing_street,
    "BillingCity" AS billing_city,
    "BillingState" AS billing_state,
    "BillingPostalCode" AS billing_postal_code,
    "BillingCountry" AS billing_country,
    "ShippingStreet" AS shipping_street,
    "ShippingCity" AS shipping_city,
    "ShippingState" AS shipping_state,
    "ShippingPostalCode" AS shipping_postal_code,
    "ShippingCountry" AS shipping_country,

    -- Business information
    "AnnualRevenue" AS annual_revenue,
    "NumberOfEmployees" AS number_of_employees,
    "Description" AS description,
    "Rating" AS rating,

    -- Custom fields (common in Salesforce)
    "Account_Status__c" AS account_status,
    "Organization_Type__c" AS organization_type,
    "Registration_Number__c" AS registration_number,
    "PAN_Number__c" AS pan_number,
    "GST_Number__c" AS gst_number,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id

FROM {{ source('staging_salesforce', 'account') }}

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
