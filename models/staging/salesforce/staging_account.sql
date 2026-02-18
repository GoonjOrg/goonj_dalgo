-- Account data extraction from Salesforce
{{ config(
    materialized='table',
    tags=['accounts', 'staging', 'salesforce', 'raw_data', 'data_extraction']
) }}

SELECT
    -- Basic system information
    "Id" AS account_id,
    "Name" AS account_name,
    "Account_Code__c" AS account_code,
    "State__c" AS state,
    "District__c" AS district,
    "Block__c" AS block,
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

    --For Sanjha Partners
    --"Partnership_type__c" as partnership_type,
    "Are_you_a_Member_of_Registered_With__c" as registered_with,
    "Contacted_Date__c" as contacted_date,
    "Date_of_Registration__c" as date_of_registration,
    "Date_of_Visit__c" as date_of_visit,
    "Description_of_Scope_of_Work__c" as description_of_scope_of_work,
    "Details_of_current_and_past_funding__c" as funding_information,
    "FCRA__c" AS has_fcra,
    "Last_FVR_Notified__c" AS last_fvr_notified,
    "Last_OPF_Notified__c" AS last_opf_notified,
    "Onboard_Stage__c" AS onboard_stage,
    --"Partnership_type__c" as partnership_type,


    -- Custom fields (common in Salesforce) - commented out if not available
    -- "Account_Status__c" AS account_status,
    -- "Organization_Type__c" AS organization_type,
    -- "Registration_Number__c" AS registration_number,
    -- "PAN_Number__c" AS pan_number,
    -- "GST_Number__c" AS gst_number,

    -- System integration information
    "_airbyte_raw_id" AS airbyte_raw_id

FROM {{ source('staging_salesforce', 'account') }}

WHERE
    -- Don't include deleted records
       "IsDeleted" = FALSE or "IsDeleted" = false or "IsDeleted" = False
       AND "Type" !='Household'


ORDER BY "CreatedDate" DESC, "Id"
